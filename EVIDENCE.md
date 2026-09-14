## 1. CLI: bundle validates and deploys

Capture these commands succeeding:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
# Skipping pre_migrations and orchestrator in the original dev evidence to save resources.
```
<img width="734" height="247" alt="deploy working - dev env" src="https://github.com/user-attachments/assets/ff3e03fd-1af7-4496-b015-c630343a2dd9" />

```bash
databricks bundle validate -t test
databricks bundle deploy -t test
databricks bundle run -t test pre_migrations
databricks bundle run -t test telematics_orchestrator
```
<img width="840" height="567" alt="deploy working - test env" src="https://github.com/user-attachments/assets/eb93a757-984f-4553-a817-983adfcf4a01" />

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run -t prod pre_migrations
databricks bundle run -t prod telematics_orchestrator
```
<img width="819" height="424" alt="deploy working - prod" src="https://github.com/user-attachments/assets/5c92d5ed-a85a-4b91-8cf1-f081705c800a" />

## 2. Workspace assets

Show the target-specific pipeline/job and the pipeline graph containing:

- `bronze_pings`
- `silver_pings`
- `gold_pings_enriched`
- `gold_truck_current`

<img width="1040" height="383" alt="successful pipeline - prod" src="https://github.com/user-attachments/assets/767b7db5-0fd7-4ac6-b257-755adf6839ba" />

## 3. Gold data

Run:

```sql
SELECT *
FROM telematics.prod.gold_truck_current
ORDER BY truck_id;
```

<img width="1269" height="677" alt="gold truck current working - prod" src="https://github.com/user-attachments/assets/6e77d4b3-c003-4875-a476-6d3612bd744d" />

Show coordinates plus driver/depot/region.

## 4. Dedup/data quality

The generator writes an exact duplicate and a row with a null latitude in every batch. Run this query to
return those problematic Bronze rows and show what reached Silver:

```sql
WITH bronze_typed AS (
  SELECT
    truck_id,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    TO_TIMESTAMP(event_ts) AS event_ts,
    _ingested_at,
    _source_file
  FROM telematics.prod.bronze_pings
),
bronze_assessed AS (
  SELECT
    *,
    COUNT(*) OVER (
      PARTITION BY truck_id, event_ts, latitude, longitude
    ) AS bronze_key_count
  FROM bronze_typed
),
silver_counts AS (
  SELECT
    truck_id,
    event_ts,
    latitude,
    longitude,
    COUNT(*) AS silver_key_count
  FROM telematics.prod.silver_pings
  GROUP BY truck_id, event_ts, latitude, longitude
)
SELECT
  b.truck_id,
  b.event_ts,
  b.latitude,
  b.longitude,
  b._ingested_at,
  b._source_file,
  b.bronze_key_count,
  COALESCE(s.silver_key_count, 0) AS silver_key_count,
  COALESCE(s.silver_key_count, 0) > 0 AS in_silver,
  CASE
    WHEN b.truck_id IS NULL OR b.event_ts IS NULL THEN 'REMOVED_REQUIRED_FIELD'
    WHEN b.latitude IS NULL OR b.longitude IS NULL
      OR b.latitude NOT BETWEEN -90.0 AND 90.0
      OR b.longitude NOT BETWEEN -180.0 AND 180.0
      THEN 'REMOVED_INVALID_COORDINATES'
    WHEN b.bronze_key_count > 1 THEN 'DUPLICATES_COLLAPSED_TO_ONE'
  END AS silver_disposition
FROM bronze_assessed AS b
LEFT JOIN silver_counts AS s
  ON b.truck_id <=> s.truck_id
 AND b.event_ts <=> s.event_ts
 AND b.latitude <=> s.latitude
 AND b.longitude <=> s.longitude
WHERE b.bronze_key_count > 1
   OR b.truck_id IS NULL
   OR b.event_ts IS NULL
   OR b.latitude IS NULL
   OR b.longitude IS NULL
   OR b.latitude NOT BETWEEN -90.0 AND 90.0
   OR b.longitude NOT BETWEEN -180.0 AND 180.0
ORDER BY b._ingested_at DESC, b.truck_id;
```

For a valid duplicate key, expect `bronze_key_count` to be greater than one,
`silver_key_count` to be one, and `in_silver` to be true. This means Silver retained one
copy of the event. For an invalid row, expect `silver_key_count` to be zero and
`in_silver` to be false, showing that the expectation removed it.

In the successful pipeline run you can see Bronze has 20 more rows than Silver, due to 10 invalid-coordinate rows
and 10 duplicate rows being removed.

<img width="1337" height="784" alt="dropped rows from silver - prod" src="https://github.com/user-attachments/assets/b6ec4e2a-cb4b-4856-951d-1c10dab276b5" />

## 5. Migration history proof

After the next clean deployment, show:

```sql
SELECT *
FROM telematics.prod._migrations
ORDER BY applied_at;
```

The existing screenshots below were captured before the history table was renamed from `_schema_migrations` to
`_migrations`; replace them with fresh screenshots after the planned destroy/redeploy.

<img width="1254" height="320" alt="migration history - prod" src="https://github.com/user-attachments/assets/8fc6e537-87b5-4761-b6e9-984430bb42b8" />

<img width="1595" height="595" alt="migrations working - prod env" src="https://github.com/user-attachments/assets/24325d56-297d-4809-9a65-54bf63ba9c72" />

The baseline history should include table creation, the original `active_flag` addition, and the fleet seed.
After promoting this branch, it should also contain `20260914_00_drop_active_flag` with phase `post`.

## 6. Post-migration contraction demonstration

This branch demonstrates a destructive schema contraction while keeping the normal deployment procedure unchanged.
Start from the baseline/main version where `active_flag` exists and is used by the Gold pipeline.

Before deployment, prove the column exists:

```sql
DESCRIBE TABLE telematics.dev.truck_details;

SELECT truck_id, active_flag
FROM telematics.dev.truck_details
ORDER BY truck_id;
```

Then run the complete standard release block for dev:

```bash
databricks bundle validate -t dev
databricks bundle sync -t dev
databricks bundle run -t dev pre_migrations
databricks bundle deploy -t dev
databricks bundle run -t dev post_migrations
```

For this release, the pre phase has no new migration. The deploy installs pipeline code that no longer references
`active_flag`, and the post phase runs `20260914_00_drop_active_flag.sql` to remove the obsolete reference-table
column. Do not insert a compatibility run or full refresh into the middle of this five-step release block.

After the release block completes, run the release-specific operational refresh:

```bash
databricks bundle run -t dev telematics_pipeline --full-refresh-all
```

Then optionally run the normal orchestrator as final verification:

```bash
databricks bundle run -t dev telematics_orchestrator
```

Promote using the same five-step release block for `test` and `prod`, followed by the full refresh for each target.

After the post migration, prove the persistent column is gone and the migration was recorded:

```sql
DESCRIBE TABLE telematics.prod.truck_details;

SELECT *
FROM telematics.prod._migrations
WHERE migration_id = '20260914_00_drop_active_flag'
ORDER BY applied_at;
```

Finally show that Gold still populates successfully and no longer exposes `active_flag`:

```sql
SELECT *
FROM telematics.prod.gold_truck_current
ORDER BY truck_id;
```

The walkthrough explanation is: **the automated release procedure is fixed and always runs validate -> sync -> pre ->
deploy -> post.** This release deploys code that no longer depends on `active_flag` before its post migration drops the
persistent column. The full refresh is a separate operational action after release, required to reconcile the
pipeline-managed streaming-table schema.

## 7. Optional schema-drift proof

Run prod with:

```bash
databricks bundle run -t prod --params drift_mode=rename_latitude,batches=3 telematics_orchestrator
```

**Ran out of resources attempting this.**

Inspect recent Bronze rows and the pipeline expectation metrics for the `latitude` issue. Explain that Bronze tolerates
the drift, while curated contract changes require a reviewed code/migration change.
