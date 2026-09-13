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

A new environment first deploys the bundle so the migration jobs exist, then replays the retained SQL migration
chain before its first pipeline execution. This makes the repository capable of reconstructing the current
managed reference/schema state from an empty target without manual SQL edits.

Migration files remain **SQL-only** under `src/migrations/pre` and `src/migrations/post`. Each target maintains
its own `<catalog>.<schema>._migrations` Delta table containing migration ID, checksum, phase, applied time,
bundle target, and commit SHA.

The runner loads applied IDs once, executes pending files in deterministic filename order, records a migration
only after success, and fails if the checksum of an already-applied migration changes. Applied files remain in
Git forever. One top-level SQL statement is allowed per file; multi-step logic can use a `BEGIN ... END` block.
The included `active_flag` migration checks `information_schema.columns` before issuing `ALTER TABLE`, and seed
or backfill work uses idempotent `MERGE` patterns where appropriate.

Rollback is **forward-fix by default**: applied migrations are immutable. A destructive reverse operation is a
new reviewed migration, and data-impacting rollback would use Delta history/restore only when retention and
business requirements make that safe.

## Pipeline refresh and rebuilds

Full refresh is deliberately **not modeled as a migration**. SQL migrations represent durable structural or
reference-data state; a Lakeflow full refresh is an operational transition that recomputes derived state.

When a deployed pipeline schema/logic change requires historical Gold rows to be recalculated, an explicit full
or selective refresh is acceptable. That step can be performed with the bundle CLI, while Bronze/Silver remain
the replay source. A freshly recreated target does not need the transition-only refresh because its first normal
pipeline run builds derived tables from the current definitions after all SQL migrations have replayed.

## Scale

No partitioning is added for the tiny demo. At hundreds of thousands of trucks, the same Auto Loader/Delta
shape remains viable; the team would tune cadence and watermark from observed latency, use clustering where
query patterns justify it, monitor streaming state/backlog, and revisit the broadcast strategy if the truck
dimension becomes large. The key principle is stable: **Bronze preserves recoverability, Silver enforces the
trusted contract, Gold serves business semantics, and production schema/reference changes are promoted as code.**

## Future Azure DevOps deployment enhancements

These are planned release/PR controls rather than requirements of the current take-home:

- Detect bootstrap vs existing-environment deployment so pre-migrations run at the correct point without manual judgment.
- Serialize releases and fail closed on migration, deployment, or pipeline errors.
- Add PR validation against the proposed merged tree to reject duplicate migration IDs/timestamp prefixes.
- Preserve runtime checksum validation so an already-applied migration can never be silently edited.
