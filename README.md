# Truck Telematics - Databricks Declarative Automation Bundle

A self-contained take-home implementation for a real-time truck GPS pipeline on Databricks Free Edition.
It uses **Lakeflow Spark Declarative Pipelines**, **Auto Loader**, a **stream-static join**, a current-state
**materialized view**, and a small **migration runner** that promotes table changes through dev -> test -> prod.

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
|   |-- pipeline.yml
|   `-- storage.yml
|-- src/
|   |-- jobs/
|   |   |-- generate_pings.py
|   |   `-- migration_runner.py
|   |-- migrations/
|   |   `-- pre/
|   |-- pipeline/
|   |   `-- telematics_pipeline.py
|   `-- sql/
|       `-- sample_queries.sql
|-- DESIGN.md
`-- EVIDENCE.md
```

## Prerequisites

1. A Databricks Free Edition workspace.
2. A recent Databricks CLI. This project explicitly uses the **direct** bundle deployment engine and
   native Unity Catalog schema/volume resources.
3. Authentication configured for the workspace (`databricks auth login` or `databricks configure`).
4. A writable Unity Catalog catalog named `telematics`.

The exercise itself assumes the `telematics` catalog. If your Free Edition account cannot create or use it,
change the top-level `catalog` variable in `databricks.yml` to an existing writable catalog. The bundle owns
and creates the target-specific **schemas** and **landing volume**; the catalog is intentionally treated as
shared platform infrastructure because dev/test/prod all live under the same catalog.

Sanity check:

```bash
databricks current-user me
```

## Validate and deploy

There are two deployment flows: a one-time bootstrap for a new target and the normal
release flow after the pre-migration job exists.

For the **first deployment** of each target, only validate and deploy the bundle so
Databricks can create the `pre_migrations` job. Do not run migrations or the orchestrator
as part of this bootstrap step. After bootstrap, use the subsequent release flow below
to apply migrations and refresh the pipeline.

**First deployment - dev:**

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

**First deployment - test:**

```bash
databricks bundle validate -t test
databricks bundle deploy -t test
```

**First deployment - prod:**

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

For **later releases**, validate the bundle, sync the new source files, run pre-migrations,
and then deploy the resource definitions. The sync is necessary
because the already-deployed pre-migration job must be able to see the migration files
from the new release before the full bundle deployment.

**Subsequent release - dev:**

```bash
databricks bundle validate -t dev
databricks bundle sync -t dev
databricks bundle run -t dev pre_migrations
databricks bundle deploy -t dev
databricks bundle run -t dev telematics_orchestrator
```

**Subsequent release - test:**

```bash
databricks bundle validate -t test
databricks bundle sync -t test
databricks bundle run -t test pre_migrations
databricks bundle deploy -t test
databricks bundle run -t test telematics_orchestrator
```

**Subsequent release - prod:**

```bash
databricks bundle validate -t prod
databricks bundle sync -t prod
databricks bundle run -t prod pre_migrations
databricks bundle deploy -t prod
databricks bundle run -t prod telematics_orchestrator
```

In sequence, the normal release flow is:

```text
validate -> sync -> pre-migrations -> deploy -> orchestrator
```

`bundle run` takes a deployed resource key, not a Python filename. Sync uploads the new
migration SQL and runner before invoking the existing pre-migration job. If the migration
job definition or its arguments change, deploy those compatible changes first. Keep the
orchestrator paused and wait for active runs to finish before syncing; sync updates application
files too. Serialize releases per target, stop on any failed command, and resume schedules only
after successful migrations. Job concurrency limits do not lock other jobs or deployments.

Optionally pass `--params commit_sha=<commit>` to the migration job for traceability.

The test target contains an hourly cron and prod a 15-minute cron. Both are delivered **PAUSED** so deploying
this take-home cannot unexpectedly consume Free Edition quota. Dev is manual. Change `pause_status` to
`UNPAUSED` when you intentionally want those schedules active.

All Databricks compute in this bundle is serverless. The pipeline declares `serverless: true` explicitly.
For Python job tasks, Databricks expresses serverless compute with an `environment_key` and no classic cluster
configuration; the bundle therefore intentionally has no `new_cluster`, `job_clusters`, or
`existing_cluster_id` settings.

## What the orchestrator does

Every scheduled or manual orchestrator run performs:

1. `generate_pings` - write synthetic JSON files under the target landing volume.
2. `refresh_pipeline` - run the triggered Lakeflow pipeline.

The separate, unscheduled `pre_migrations` job applies SQL changes during release. Run it before
the first orchestrator run to create and seed the reference table. The job has
`max_concurrent_runs: 1`; the release process must still avoid overlapping it with deployments.

This take-home implements only pre-migrations to reduce Databricks Free Edition compute use.
Pre-migrations run before application deployment and are best suited to **expansion** changes,
such as adding tables or columns that both the old and new application versions can tolerate.
A production release process would commonly add a post-migration phase after deployment for
**contraction** changes, such as removing obsolete columns or tables after the new application
version no longer depends on them. Contraction migrations require extra care because rollback
may need the removed schema or data.

## Migration behavior

Migrations live permanently in source control and are named with a timestamp plus a readable slug:

```text
20260908_170000_create_truck_details.sql
20260908_171500_add_active_flag.sql
20260908_173000_seed_truck_details.sql
```

Each environment has its own table:

```text
telematics.dev._schema_migrations
telematics.test._schema_migrations
telematics.prod._schema_migrations
```

The runner reads that table once, checks each migration checksum, skips migrations already applied, executes
pending files in filename order, and records a migration **only after success**. An already-applied migration
whose file contents changed fails fast; add a new migration instead of editing history.

Each file contains one top-level Databricks SQL statement. Multi-statement work can be wrapped in a
`BEGIN ... END` scripting block. The `active_flag` migration demonstrates a safe column-add by checking
`information_schema.columns` before issuing the `ALTER TABLE`.

Run just pre-migrations if you want to inspect the framework directly:

```bash
databricks bundle run -t dev pre_migrations
```

The seed migration inserts the 20 explicit demo trucks only when their IDs are missing.
It preserves existing truck attributes, including on environments seeded by the former Python task.
After success it is skipped using migration history; ongoing reference updates are separate from seeding.

## Schema drift demo

The generator can intentionally simulate two upstream changes:

```bash
# New source field: Auto Loader evolves Bronze schema.
databricks bundle run -t dev \
  --params drift_mode=add_column,batches=3 \
  telematics_orchestrator

# Rename latitude -> lat: Bronze retains the drift; inspect pipeline expectation
# metrics and recent Bronze rows to observe the missing latitude values.
databricks bundle run -t dev \
  --params drift_mode=rename_latitude,batches=3 \
  telematics_orchestrator
```

Bronze uses Auto Loader `addNewColumns`. A new column can cause the first update to stop after Auto Loader
persists the evolved schema. The pipeline task therefore has retries configured. Silver and Gold do **not**
automatically adopt arbitrary new business fields; those remain reviewed schema-contract changes.

## Rebuild / checkpoint behavior

Lakeflow manages the streaming state/checkpoints. Normal stop/restart keeps that state and continues
incrementally. Bronze is the retained replay source. If Silver logic needs historical correction, intentionally
reset/recompute the pipeline rather than treating the checkpoint as data:

```bash
databricks bundle run -t dev telematics_pipeline --full-refresh-all
```

You can also selectively full refresh named tables with the current bundle CLI.

## Query the result

Open `src/sql/sample_queries.sql`, or run equivalent queries in the SQL editor. The core proof query is:

```sql
SELECT *
FROM telematics.dev.gold_truck_current
ORDER BY truck_id;
```

Expected result: up to one current row for each truck with GPS coordinates plus driver/depot/region details.

## CI stretch

`.github/workflows/deploy-dev.yml` validates and deploys the dev target on push, then runs pre-migrations.
This deploy-first sequence supports fresh environments; schedules remain paused. Add repository secrets:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

For a real production system, prefer GitHub OIDC/workload identity with a service principal and protect the
prod environment with an approval gate.

## Notes for the live walkthrough

Be ready to explain:

- Why Lakeflow was chosen over hand-rolled Structured Streaming.
- Why the dedup key is `truck_id + event_ts + latitude + longitude` and why the watermark is 10 minutes.
- The difference between the event-level stream-static enrichment and the current-state materialized view.
- Why schema drift is tolerated in Bronze but curated schema changes are reviewed/migrated.
- How `_schema_migrations` makes promotion apply-once and traceable.
- Why a full refresh rebuilds derived data from Bronze rather than treating checkpoints as the source of truth.
