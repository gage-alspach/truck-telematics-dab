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

`truck_details` is a small managed Delta reference table created and seeded by pre-migrations.
The seed contains 20 explicit records matching the original fixed-seed generator and uses an insert-only
`MERGE`: existing truck attributes are preserved, including on retries. The scheduled job does not reseed it. `gold_pings_enriched` performs the required **stream-static join** between streaming Silver pings and
the static dimension; the tiny dimension is explicitly broadcast. This event-level table reflects the static
snapshot used while each ping is processed.

`gold_truck_current` is a materialized view that selects the latest valid Silver ping per truck and joins the
current `truck_details` snapshot again. This gives the business-facing current-position result and means a
reference-data correction is reflected on the next refresh even if a truck has not emitted a new ping.

## Schema migrations as code

Migrations run in a separate unscheduled `pre_migrations` job, not in the scheduled orchestrator.
Bootstrap deploys resources first so the migration job exists. Later releases sync files, run
pre-migrations, and then deploy. Keep scheduled work paused and serialize releases while doing this
because sync changes deployed application files too.

This take-home implements only the pre phase to reduce Databricks Free Edition compute use.
Pre-migrations are intended for expansion changes, such as adding columns or tables before the new
application version starts using them. A production release process would commonly add a post phase
for contraction changes, such as dropping obsolete columns or tables after deployment proves the new
version no longer depends on them. Contractions should be reviewed for rollback and data-retention risk.

Intentional table changes use timestamp-named SQL migrations in `src/migrations/pre`. Sequential
integers are avoided so several engineers can create changes concurrently without coordinating the next
number. Each target maintains its own `<catalog>.<schema>._schema_migrations` table containing migration ID,
checksum, phase, applied time, bundle target, and commit SHA.

The migration runner loads applied IDs once, executes pending files in deterministic filename order, records a
migration only after success, and fails if the checksum of an already-applied migration changes. Applied files
remain in Git forever. One top-level SQL statement is allowed per file; multi-step logic can use a
`BEGIN ... END` block. The included `active_flag` migration checks `information_schema.columns` before issuing
`ALTER TABLE`, so rerunning the migration logic is safe and non-destructive.

Rollback is **forward-fix by default**: applied migrations are immutable. A destructive reverse operation is a
new reviewed migration, and data-impacting rollback would be based on Delta history/restore only when the
business and retention requirements justify it.

## Scale

No partitioning is added for the tiny demo. At hundreds of thousands of trucks, the same Auto Loader/Delta
shape remains viable; the team would tune cadence and watermark from observed latency, use clustering where
query patterns justify it, monitor streaming state/backlog, and revisit the broadcast strategy if the truck
dimension becomes large. The key principle is stable: **Bronze preserves recoverability, Silver enforces the
trusted contract, Gold serves business semantics, and production schema changes are promoted as code.**

## Future Azure DevOps deployment enhancements

These are planned ADO release/PR checks, not implemented by the current bundle or GitHub workflow.

- Before deployment, check whether the target's pre_migrations job exists using deployment metadata or a known job ID. If present, publish the current migration files and run it; fail the release if it fails. If confirmed missing, log the bootstrap condition and skip the pre-deployment invocation. Authentication, permission, and service errors must fail the check rather than be treated as a missing job.
- Preserve first-deployment prerequisites: this project's pre-migrations create and seed truck_details. If the pre-deployment invocation was skipped, run pre_migrations after deployment and before the first orchestrator run. A future independently provisioned migration framework can remove this bootstrap dependency.
- Add a post-migration phase if the release process begins performing contraction changes. Run it after deployment and gate destructive changes on compatibility, rollback, and retention checks.
- Add PR validation against the proposed merged tree to reject duplicate migration IDs (filename stems). Exact duplicate filenames cannot coexist in a Git directory, so also reject duplicate timestamp prefixes with different descriptions to catch concurrent authors choosing the same identifier. Report the conflicting paths and require renaming unapplied migrations before merge. Keep the existing runtime content-checksum protection; filename uniqueness serves a different purpose.
