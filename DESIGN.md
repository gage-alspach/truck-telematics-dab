# DESIGN - Real-Time Truck Telematics Pipeline

## Core architecture

The solution uses **Lakeflow Spark Declarative Pipelines** instead of hand-rolled Structured Streaming. The
workload maps cleanly to a Bronze -> Silver -> Gold graph, and Lakeflow manages streaming state/checkpoints and
dataset dependencies while still exposing the underlying Auto Loader and Spark semantics. The pipeline is
**triggered**, not continuous: latency should be a business requirement, and this demo does not need
sub-second processing. Dev is manual; test is configured hourly and prod every 15 minutes. Test/prod schedules
are checked into the bundle but paused by default to protect Free Edition quota.

All targets share one existing `telematics` catalog and use separate schemas: `dev`, `test`, and `prod`.
The bundle creates each target schema and managed landing volume, and parameterizes names/paths by target.

## Bronze and schema drift

Synthetic JSON files land in `/Volumes/telematics/<target>/landing/pings`. Auto Loader writes the append-only
`bronze_pings` streaming table and stores evolving schema metadata under a target-specific schema location.
Bronze keeps source values close to raw and adds ingestion/source-file metadata. New columns can evolve the
Bronze schema; incompatible values remain observable in `_rescued_data` rather than being silently lost.

Schema drift does **not** automatically change the trusted Silver/Gold contract. A source rename such as
`latitude -> lat` remains visible in Bronze and in the pipeline's expectation metrics, but an engineer still
decides whether and how to change the curated model. The take-home omits a separate post-run quality-report
task to reduce Databricks Free Edition compute use. A production system could send these metrics to a
monitoring destination without adding a dedicated task to every pipeline run.

## Silver correctness and replay

`silver_pings` explicitly casts the four source fields, drops invalid/missing coordinates with Lakeflow
expectations, and deduplicates on `truck_id + event_ts + latitude + longitude`. A 10-minute event-time watermark
bounds streaming deduplication state; this is a demo assumption that should be replaced by measured source
lateness in production.

Lakeflow owns normal checkpoint/restart state. Bronze is the replay source, so a logic change that requires
historical correction is handled with an intentional full refresh/rebuild from retained Bronze rather than by
manually manipulating checkpoints.

## Static reference and Gold

`truck_details` is a small managed Delta reference table created and seeded by migrations. The seed contains 20
explicit records matching the fixed generator IDs and uses an insert-only `MERGE`, so existing truck attributes
are preserved on retries. The scheduled ingestion job does not reseed it.

`gold_pings_enriched` performs the required **stream-static join** between streaming Silver pings and the static
dimension; the tiny dimension is explicitly broadcast. This event-level table reflects the static snapshot used
while each ping is processed.

`gold_truck_current` is a materialized view that selects the latest valid Silver ping per truck and joins the
current `truck_details` snapshot again. This gives the business-facing current-position result and means a
reference-data correction is reflected on the next refresh even if a truck has not emitted a new ping.

## SQL migrations as code

Migrations are separate unscheduled release jobs rather than tasks in the recurring ingestion workflow. The
runner supports two phases:

- **pre** - backward-compatible structure required before the new pipeline version is deployed
- **post** - SQL backfill or contraction work that should occur only after deployment succeeds

A normal existing-environment release is therefore:

```text
validate -> sync migration files -> pre migrations -> deploy -> post migrations -> pipeline run/refresh
```

For destructive post migrations, the post step can be deliberately gated on a successful run of the newly
deployed code before the contraction is applied. Schedules remain paused and releases are serialized while that
compatibility check is performed.

A new environment first deploys the bundle so the migration jobs exist, then replays the retained SQL migration
chain before its first pipeline execution. This makes the repository capable of reconstructing the current
managed reference/schema state from an empty target without manual SQL edits.

Migration files remain **SQL-only** under `src/migrations/pre` and `src/migrations/post`. Each target maintains
its own `<catalog>.<schema>._migrations` Delta table containing migration ID, checksum, phase, applied time,
bundle target, and commit SHA.

The runner loads applied IDs once, executes pending files in deterministic filename order, records a migration
only after success, and fails if the checksum of an already-applied migration changes. Applied files remain in
Git forever. One top-level SQL statement is allowed per file; multi-step logic can use a `BEGIN ... END` block.
The baseline `active_flag` migration checks `information_schema.columns` before issuing `ALTER TABLE`, and seed
or backfill work uses idempotent patterns where appropriate.

Rollback is **forward-fix by default**: applied migrations are immutable. A destructive reverse operation is a
new reviewed migration, and data-impacting rollback would use Delta history/restore only when retention and
business requirements make that safe.

## Migration demonstration

This branch demonstrates a real **contract/post-migration** release by removing the obsolete `active_flag`
attribute.

1. The baseline environment already contains `truck_details.active_flag`, and the baseline pipeline selects it
   into both Gold outputs.
2. This branch changes the pipeline definition so neither Gold output references `active_flag`.
3. The updated pipeline is deployed first.
4. Because hard deletion of an output column from a pipeline-managed streaming table is not checkpoint-compatible,
   the pipeline is explicitly full-refreshed to reconcile the declarative Gold schema and prove the new version no
   longer depends on `active_flag`.
5. Post migration `20260914_00_drop_active_flag.sql` then enables Delta column mapping on the static
   `truck_details` table and drops `active_flag`.
6. A normal pipeline run after the post migration verifies that the deployed application continues to work after
   the contraction.

This sequencing demonstrates why a destructive drop belongs in **post**, not pre: dropping the source/reference
column before deploying compatible code could break the existing pipeline. Delaying the contraction preserves a
safe deployment boundary and makes rollback easier until the new code has been proven.

The migration is idempotent because it first checks `information_schema.columns`; if `active_flag` is already
absent, it performs no action. Column mapping is enabled because Databricks requires it for metadata-only Delta
column drops.

A freshly recreated environment remains reproducible. Baseline pre-migrations create and seed `truck_details`
with `active_flag`, the current pipeline definition is deployed without that dependency, and the retained post
migration removes the obsolete persistent column. No manual SQL edit is required.

## Pipeline refresh and rebuilds

Full refresh is deliberately **not modeled as a migration**. SQL migrations represent durable structural or
reference-data state; a Lakeflow full refresh is an operational transition that recomputes derived state.

When a deployed pipeline schema/logic change requires historical Gold rows to be recalculated, an explicit full
or selective refresh is acceptable. That step can be performed with the bundle CLI, while Bronze/Silver remain
the replay source.

The tradeoff is intentional: some operational recovery actions remain manual, but the durable environment state
is reproducible. In a worst-case rebuild, the bundle recreates managed resources, the retained migration chain
reconstructs the current schema/reference state, and the current declarative pipeline definition rebuilds derived
state from retained source data or newly generated demo input. A freshly recreated target therefore does not need
a transition-only refresh for historical compatibility; its first run builds derived tables from the current code.

## Scale

No partitioning is added for the tiny demo. At hundreds of thousands of trucks, the same Auto Loader/Delta
shape remains viable; the team would tune cadence and watermark from observed latency, use clustering where
query patterns justify it, monitor streaming state/backlog, and revisit the broadcast strategy if the truck
dimension becomes large. The key principle is stable: **Bronze preserves recoverability, Silver enforces the
trusted contract, Gold serves business semantics, and production schema/reference changes are promoted as code.**
