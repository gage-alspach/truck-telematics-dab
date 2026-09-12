# Evidence checklist

Use this as the shot list for the 3–5 minute recording or screenshots.

## 1. CLI: bundle validates and deploys

Capture these commands succeeding:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev telematics_orchestrator

databricks bundle validate -t test
databricks bundle deploy -t test
databricks bundle run -t test telematics_orchestrator

databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run -t prod telematics_orchestrator
```

## 2. Workspace assets

Show the target-specific pipeline/job and the pipeline graph containing:

- `bronze_pings`
- `silver_pings`
- `gold_pings_enriched`
- `gold_truck_current`

## 3. Gold data

Run:

```sql
SELECT *
FROM telematics.prod.gold_truck_current
ORDER BY truck_id;
```

Show coordinates plus driver/depot/region.

## 4. Dedup/data quality

Show that the generator writes a deliberate duplicate and bad latitude, then query Silver for duplicate groups
(or show Lakeflow expectation metrics).

## 5. Migration proof

Show:

```sql
SELECT *
FROM telematics.prod._schema_migrations
ORDER BY applied_at;
```

The history should include both the table-creation migration and the `active_flag` migration.

For the strongest promotion story, create one additional timestamped migration after your baseline commit,
for example `YYYYMMDD_HHMMSS_add_reference_note.sql`, deploy/run it in dev, then test, then prod, and show the
new column plus one new migration-history row in each environment. Do not make any manual prod edit.

## 6. Optional schema-drift proof

Run dev with:

```bash
databricks bundle run -t dev \
  --params drift_mode=rename_latitude,batches=3 \
  telematics_orchestrator
```

Capture the quality report warning for the recent `latitude` null-rate spike. Explain that Bronze tolerates the
drift, while curated contract changes require a reviewed code/migration change.
