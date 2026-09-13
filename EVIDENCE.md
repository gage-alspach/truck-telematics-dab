## 1. CLI: bundle validates and deploys

Capture these commands succeeding:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
#Skipping pre_migrations and orchestrator in the dev env to save resources 

databricks bundle validate -t test
databricks bundle deploy -t test
databricks bundle run -t test pre_migrations
databricks bundle run -t test telematics_orchestrator

databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run -t prod pre_migrations
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

## 5. Migration proof

Show:

```sql
SELECT *
FROM telematics.prod._schema_migrations
ORDER BY applied_at;
```

The history should include the table-creation, `active_flag`, and initial fleet seed migrations.

For the strongest promotion story, create one additional timestamped migration after your baseline commit,
for example `YYYYMMDD_HHMMSS_add_reference_note.sql`, deploy/run it in dev, then test, then prod, and show the
new column plus one new migration-history row in each environment. Do not make any manual prod edit.

## 6. Optional schema-drift proof

Run prod with:

```bash
databricks bundle run -t prod --params drift_mode=rename_latitude,batches=3 telematics_orchestrator
```
Ran out of resources attempting this.
Inspect recent Bronze rows and the pipeline expectation metrics for the `latitude` issue. Explain that Bronze tolerates the drift, while curated contract changes require a reviewed code/migration change.
