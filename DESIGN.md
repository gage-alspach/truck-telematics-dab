# DESIGN — Real-Time Truck Telematics Pipeline

## Core architecture

The solution uses **Lakeflow Spark Declarative Pipelines** instead of hand-rolled Structured Streaming. The
workload maps cleanly to a Bronze → Silver → Gold graph, and Lakeflow manages streaming state/checkpoints and
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

Schema drift does **not** automatically change the trusted Silver/Gold contract. A lightweight post-run health
report surfaces rescued-data rate and null spikes in expected Bronze fields. A source rename such as
`latitude -> lat` therefore becomes visible quickly, but an engineer still decides whether/how to change the
curated model. In production these metrics could feed a job notification or monitoring destination.

## Silver correctness and replay

`silver_pings` explicitly casts the four source fields, drops invalid/missing coordinates with Lakeflow
expectations, and deduplicates on `truck_id + event_ts + latitude + longitude`. A 10-minute event-time watermark
bounds streaming deduplication state; this is a demo assumption that should be replaced by measured source
lateness in production.

Lakeflow owns normal checkpoint/restart state. Bronze is the replay source, so a logic change that requires
historical correction is handled with an intentional full refresh/rebuild from retained Bronze rather than by
manually manipulating checkpoints.

## Static reference and Gold

`truck_details` is a small managed Delta reference table created by migration and seeded idempotently with
`MERGE`. `gold_pings_enriched` performs the required **stream-static join** between streaming Silver pings and
the static dimension; the tiny dimension is explicitly broadcast. This event-level table reflects the static
snapshot used while each ping is processed.

`gold_truck_current` is a materialized view that selects the latest valid Silver ping per truck and joins the
current `truck_details` snapshot again. This gives the business-facing current-position result and means a
reference-data correction is reflected on the next refresh even if a truck has not emitted a new ping.

## Schema migrations as code

Intentional table changes use timestamp-named SQL migrations in `src/migrations/pre` and `post`. Sequential
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
