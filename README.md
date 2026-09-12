# Truck Telematics — Databricks Declarative Automation Bundle

A self-contained take-home implementation for a real-time truck GPS pipeline on Databricks Free Edition.
It uses **Lakeflow Spark Declarative Pipelines**, **Auto Loader**, a **stream-static join**, a current-state
**materialized view**, and a small **migration runner** that promotes table changes through dev → test → prod.

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
├── databricks.yml
├── resources/
│   ├── job.yml
│   ├── pipeline.yml
│   └── storage.yml
├── src/
│   ├── jobs/
│   │   ├── generate_pings.py
│   │   ├── migration_runner.py
│   │   ├── quality_report.py
│   │   └── seed_reference.py
│   ├── migrations/
│   │   ├── pre/
│   │   └── post/
│   ├── pipeline/
│   │   └── telematics_pipeline.py
│   └── sql/
│       └── sample_queries.sql
├── DESIGN.md
└── EVIDENCE.md
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

Start with dev:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Run the entire thin slice. Passing the current Git SHA is optional but makes migration history nicer:

```bash
databricks bundle run -t dev \
  --params commit_sha=$(git rev-parse HEAD),drift_mode=none \
  telematics_orchestrator
```

Then promote the **same code**:

```bash
databricks bundle validate -t test
databricks bundle deploy -t test
databricks bundle run -t test telematics_orchestrator

databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run -t prod telematics_orchestrator
```

The test target contains an hourly cron and prod a 15-minute cron. Both are delivered **PAUSED** so deploying
this take-home cannot unexpectedly consume Free Edition quota. Dev is manual. Change `pause_status` to
`UNPAUSED` when you intentionally want those schedules active.

## What the orchestrator does

Every run performs the same ordered workflow:

1. `pre_migrations` — create migration history if needed and apply pending pre-migrations.
2. `seed_reference` — idempotent `MERGE` of the 20-row `truck_details` dimension.
3. `generate_pings` — write synthetic JSON files under the target landing volume.
4. `refresh_pipeline` — run the triggered Lakeflow pipeline.
5. `post_migrations` — apply any pending post-migrations.
6. `quality_report` — print recent rescued-data and expected-column null-rate metrics.

The job has `max_concurrent_runs: 1`, so migrations and seed activity for a target are serialized.

## Migration behavior

Migrations live permanently in source control and are named with a timestamp plus a readable slug:

```text
20260908_170000_create_truck_details.sql
20260908_171500_add_active_flag.sql
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
databricks bundle run -t dev --only pre_migrations telematics_orchestrator
```

## Schema drift demo

The generator can intentionally simulate two upstream changes:

```bash
# New source field: Auto Loader evolves Bronze schema.
databricks bundle run -t dev \
  --params drift_mode=add_column,batches=3 \
  telematics_orchestrator

# Rename latitude -> lat: Bronze retains the drift; the quality report should
# show a sharp latitude null-rate increase for recent rows.
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

`.github/workflows/deploy-dev.yml` validates and deploys the dev target on push. Add repository secrets:

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
