# Truck Telematics - Databricks Declarative Automation Bundle

A self-contained take-home implementation for a real-time truck GPS pipeline on Databricks Free Edition.
It uses **Lakeflow Spark Declarative Pipelines**, **Auto Loader**, a **stream-static join**, a current-state
**materialized view**, and versioned **pre/post SQL migration jobs** for controlled table changes.

## Architecture

```text
Synthetic JSON files
        |
        v
   Auto Loader
        |
  bronze_pings  -- raw, append-only, schema drift tolerated
        |
        v
  silver_pings  -- cast, validate, watermark, deduplicate
        |
        +-------------------- truck_details (static Delta table)
        |                                  |
        v                                  |
gold_pings_enriched <----------------------+   stream-static join
        |
        +--> event-level enriched history

silver_pings + current truck_details
        |
        v
 gold_truck_current   -- materialized current position per truck
```

## Repository layout

```text
.
|-- databricks.yml
|-- resources/
|   |-- job.yml
|   |-- migrations.yml
|   |-- post_migrations.yml
|   |-- pipeline.yml
|   `-- storage.yml
|-- src/
|   |-- jobs/
|   |   |-- generate_pings.py
|   |   `-- migration_runner.py
|   |-- migrations/
|   |   |-- pre/
|   |   `-- post/
|   |-- pipeline/
|   |   `-- telematics_pipeline.py
|   `-- sql/
|       `-- sample_queries.sql
|-- DESIGN.md
`-- EVIDENCE.md
```

## Prerequisites

1. A Databricks Free Edition workspace.
2. A recent Databricks CLI.
3. Authentication configured for the workspace (`databricks auth login` or `databricks configure`).
4. A writable Unity Catalog catalog named `telematics`.

The bundle owns the target-specific schemas and landing volumes. The shared `telematics` catalog is treated as
existing platform infrastructure because dev/test/prod all live in one workspace.

Sanity check:

```bash
databricks current-user me
```

## Targets

The bundle defines three targets in one workspace:

- `dev` - manual execution, schema `telematics.dev`
- `test` - hourly schedule encoded but paused, schema `telematics.test`
- `prod` - 15-minute schedule encoded but paused, schema `telematics.prod`

The schedules are deliberately paused so deploying this take-home does not consume Free Edition quota unexpectedly.
All compute is serverless. The pipeline declares `serverless: true`, and Python job tasks use serverless environments
without classic cluster configuration.

## Deployment model

There are two concerns during a release:

1. **Bundle deployment** delivers Databricks resource definitions and application/pipeline code.
2. **Migration jobs** execute versioned SQL structure/data changes in a controlled, auditable step.

Humans do not manually alter the prod schema. Changes are committed to Git and executed through the same bundle-defined
migration jobs in each environment.

### First deployment of a target

A new target must first be deployed so the migration jobs and other resources exist:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Repeat with `-t test` or `-t prod` for the other targets.

After bootstrap, run the pre-migrations before the first pipeline run:

```bash
databricks bundle run -t dev pre_migrations
databricks bundle run -t dev telematics_orchestrator
```

All SQL migrations are retained in Git, so a freshly recreated target can replay the full migration chain and
converge on the same current reference/schema state before the first pipeline run.

### Normal release flow

For an existing target, the intended order is:

```text
validate
  -> publish/sync the new migration files
  -> run pre_migrations
  -> bundle deploy
  -> run post_migrations when the release contains post-deploy work
  -> run or refresh the pipeline
```

The pre-migration job already exists from the prior deployment, so the new migration files must be made visible to that
job before invoking it. With the CLI this can be done with `bundle sync` before the pre-migration run:

```bash
databricks bundle validate -t dev
databricks bundle sync -t dev
databricks bundle run -t dev pre_migrations
databricks bundle deploy -t dev
databricks bundle run -t dev post_migrations
databricks bundle run -t dev telematics_orchestrator
```

`post_migrations` is safe to run even when a release contains no post SQL files; the runner simply reports that there
is nothing to apply. For releases that do not require post-deploy work, that step may be omitted to conserve Free
Edition resources.

The same release sequence is promoted through test and prod. Releases should be serialized per target and stopped on
any failed validation, migration, deployment, or pipeline run.

## Why pre and post migrations are separate

Pre-migrations are for changes that must exist before the new application version is deployed. Typical examples are:

- creating a new reference table
- adding a nullable column
- adding backward-compatible structure that both old and new code can tolerate

Post-migrations are SQL data/schema changes that should occur only after the new application version exists. Typical
examples are:

- backfilling a newly introduced column after compatible code is deployed
- cleanup/contraction work after the new code no longer depends on the old structure
- other controlled SQL changes that should be gated on successful deployment

This follows an expand/deploy/backfill-or-contract pattern instead of coupling schema mutation directly to
`databricks bundle deploy`.

## Migration behavior

Migration files are intentionally **SQL-only** and live permanently in source control under:

```text
src/migrations/pre/
src/migrations/post/
```

Each target keeps its own history table:

```text
telematics.dev._migrations
telematics.test._migrations
telematics.prod._migrations
```

The migration runner:

- executes `.sql` files in deterministic filename order
- records a migration only after it succeeds
- stores migration ID, checksum, phase, applied time, target, and commit SHA
- skips migrations already applied with the same checksum
- fails if an already-applied migration file was later modified

Applied migrations are immutable. A later correction is a new migration rather than an edit to migration history.
Because the repository retains the complete SQL migration chain, recreating an empty target and replaying the files
reconstructs the current managed reference/schema state without hand-written repair steps.

The existing baseline migrations create `truck_details`, add `active_flag` idempotently, and seed the 20-row reference
dimension with an insert-only `MERGE`.

## Migration demonstration

This branch demonstrates an expand/deploy/backfill change:

```text
pre/20260913_00_add_truck_class.sql
  -> add nullable truck_class

bundle deploy
  -> Gold definitions begin selecting truck_class

post/20260913_01_backfill_truck_class.sql
  -> populate LIGHT / MEDIUM / HEAVY from capacity_lbs
```

If existing historical event-level Gold must be recomputed with the new attribute, an explicit full refresh is then
performed as an operational step. A fresh environment does not need that transition-only refresh; replaying the SQL
migrations before the first normal pipeline run produces the current schema/reference state directly.

## What the orchestrator does

The scheduled/manual `telematics_orchestrator` is intentionally small:

1. `generate_pings` writes synthetic JSON files into the target landing volume.
2. `refresh_pipeline` executes the triggered Lakeflow pipeline.

Schema migrations are separate release jobs rather than tasks in the recurring ingestion workflow. This keeps normal
pipeline execution from repeatedly paying for migration-job tasks and makes release-time structural changes explicit.

## Pipeline behavior

### Bronze

`bronze_pings` uses Auto Loader with a target-specific schema location. Bronze remains close to the source, records
source-file and ingestion metadata, allows additive schema evolution, and keeps rescued data observable.

### Silver

`silver_pings` casts the expected source fields, drops missing/invalid coordinates with Lakeflow expectations, and
watermark-deduplicates on:

```text
truck_id + event_ts + latitude + longitude
```

The 10-minute watermark is a demo assumption and would be tuned from observed source lateness in production.

### Gold

`gold_pings_enriched` is the required stream-static integration. It joins streaming Silver pings to the small static
`truck_details` Delta table and broadcasts the reference side.

`gold_truck_current` is a materialized current-state view. It finds the latest valid ping per truck and joins the
current reference snapshot again, so reference corrections appear on the next refresh even when no new ping arrives.

## Schema changes to pipeline-owned Gold tables

Reference/source structures such as `truck_details` are managed with SQL migrations. Lakeflow-owned derived tables are
changed through the pipeline definition rather than by manually altering them in the SQL editor.

When a pipeline logic/schema change requires historical Gold rows to be recomputed, a **manual full or selective
pipeline refresh is an accepted operational step in this take-home design**. The refresh is not stored as a migration
because it is pipeline lifecycle control rather than durable schema/reference state. Bronze/Silver remain the replay
source rather than treating checkpoint or current Gold contents as authoritative data.

That manual operational step is an intentional tradeoff: the durable environment definition remains reproducible. In
a worst-case rebuild, the bundle recreates the managed resources, the complete SQL migration history reconstructs the
current schema/reference state, and the current pipeline definition rebuilds derived tables from the retained source
or newly generated demo input.

Example full refresh:

```bash
databricks bundle run -t dev telematics_pipeline --full-refresh-all
```

A freshly recreated target does not need this transition step: after its SQL migrations replay, the first normal
pipeline execution builds Gold from the current definitions.

## Free Edition note

The required thin slice is intentionally prioritized over optional compute-heavy demonstrations. Test/prod schedules
remain paused, migration jobs are run only during releases, and optional drift/full-refresh demonstrations may be
skipped when Free Edition quota is exhausted.
