## 1. CLI: bundle validates and deploys

Capture these commands succeeding:

```bash
databricks bundle validate -t dev
databricks bundle run -t dev pre_migrations
databricks bundle deploy -t dev
databricks bundle run -t dev post_migrations
databricks bundle run -t dev telematics_orchestrator
```
<img width="828" height="734" alt="successful deploy and run - dev" src="https://github.com/user-attachments/assets/634bf6b6-6ef1-4082-ad38-f88f0887a865" />


```bash
databricks bundle validate -t test
databricks bundle run -t test pre_migrations
databricks bundle deploy -t test
databricks bundle run -t test post_migrations
databricks bundle run -t test telematics_orchestrator
```



```bash
databricks bundle validate -t prod
databricks bundle run -t prod pre_migrations
databricks bundle deploy -t prod
databricks bundle run -t prod post_migrations
databricks bundle run -t prod telematics_orchestrator
```

## 2. Workspace assets

Show the target-specific pipeline/job and the pipeline graph containing:

- `bronze_pings`
- `silver_pings`
- `gold_pings_enriched`
- `gold_truck_current`

DEV: 
<img width="1378" height="469" alt="successful dev ping and pipeline refresh - dev" src="https://github.com/user-attachments/assets/3cc7c6bc-c66d-4867-a9c0-928d27b65544" />
<img width="2237" height="575" alt="successful pipeline run - dev" src="https://github.com/user-attachments/assets/3f2666c8-2d98-49bd-a0ee-00925ad05b01" />



## 3. Gold data

Run:

```sql
SELECT *
FROM telematics.prod.gold_truck_current
ORDER BY truck_id;
```

DEV:
<img width="1394" height="683" alt="gold current truck - dev" src="https://github.com/user-attachments/assets/19d37446-b030-406c-9cfa-6fba0e3fd609" />


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

DEV:
<img width="367" height="364" alt="silver dropping invalid rows - dev" src="https://github.com/user-attachments/assets/41668dbf-097f-4f73-90b8-91f25e3a0fbd" />
<img width="1346" height="409" alt="silver removals - dev" src="https://github.com/user-attachments/assets/bf96f975-6db5-440d-a0c6-2152064596bc" />


## 5. Migration history proof

After the next clean deployment, show:

```sql
SELECT *
FROM telematics.prod._migrations
ORDER BY applied_at;
```

DEV:
<img width="735" height="583" alt="successful pre-migration run - dev" src="https://github.com/user-attachments/assets/19e0e785-4f00-4ac3-93a3-5bf5a1e2cc7d" />
<img width="740" height="576" alt="successful post-migration run - dev" src="https://github.com/user-attachments/assets/0a7d14c4-c9e7-430d-ae1a-e4bf3aeac74c" />
<img width="1241" height="316" alt="migrations - dev" src="https://github.com/user-attachments/assets/ee6f4418-4667-45a4-b5c2-1a2fc7c91ac4" />


## 6. Post-migration contraction demonstration

This branch demonstrates the correct ordering for a destructive schema contraction. Start from the baseline/main
version where `active_flag` exists and is used by the Gold pipeline.

Before deployment, prove the column exists:

```sql
DESCRIBE TABLE telematics.dev.truck_details;

SELECT truck_id, active_flag
FROM telematics.dev.truck_details
ORDER BY truck_id;
```

Then promote the branch through each target. For dev:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev

# Removing active_flag from the Gold streaming-table schema is a hard deletion,
# so reconcile the declarative pipeline with an explicit full refresh.
databricks bundle run -t dev telematics_pipeline --full-refresh-all

# Only after the new pipeline definition has run without the dependency:
databricks bundle run -t dev post_migrations

# Final verification after the persistent reference-table contraction:
databricks bundle run -t dev telematics_orchestrator
```

Repeat the same sequence for `test` and `prod`.

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

The walkthrough explanation is: **deploy code that no longer depends on the column, prove the new declarative pipeline
state, then run the destructive post migration.** Dropping `active_flag` before deployment would risk breaking the old
pipeline and would weaken rollback options.

## 7. Optional schema-drift proof

Run prod with:

```bash
databricks bundle run -t prod --params drift_mode=rename_latitude,batches=3 telematics_orchestrator
```

**Ran out of resources attempting this.**

Inspect recent Bronze rows and the pipeline expectation metrics for the `latitude` issue. Explain that Bronze tolerates
the drift, while curated contract changes require a reviewed code/migration change.
