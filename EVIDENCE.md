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
<img width="823" height="786" alt="successful deploy and run - test" src="https://github.com/user-attachments/assets/b74f83be-1b3c-402f-a3ef-929a74de5453" />


```bash
databricks bundle validate -t prod
databricks bundle run -t prod pre_migrations
databricks bundle deploy -t prod
databricks bundle run -t prod post_migrations
databricks bundle run -t prod telematics_orchestrator
```
<img width="836" height="830" alt="successful deploy and run - prod" src="https://github.com/user-attachments/assets/b26b9a95-9f22-4e26-a974-8fadc02ff301" />

## 2. Workspace assets

Show the target-specific pipeline/job and the pipeline graph containing:

- `bronze_pings`
- `silver_pings`
- `gold_pings_enriched`
- `gold_truck_current`

DEV: 

<img width="1378" height="469" alt="successful dev ping and pipeline refresh - dev" src="https://github.com/user-attachments/assets/3cc7c6bc-c66d-4867-a9c0-928d27b65544" />
<img width="2237" height="575" alt="successful pipeline run - dev" src="https://github.com/user-attachments/assets/3f2666c8-2d98-49bd-a0ee-00925ad05b01" />

TEST:

<img width="1065" height="470" alt="successful test ping and pipeline refresh - test" src="https://github.com/user-attachments/assets/83f2aa6a-66aa-450e-8d3b-3cc3234832b3" />
<img width="1818" height="519" alt="successful pipeline run - test" src="https://github.com/user-attachments/assets/7e5d1b68-5fd4-4f3c-8fde-4bc8f0e49a20" />

PROD:

<img width="1815" height="472" alt="successful prod ping and pipeline refresh - prod" src="https://github.com/user-attachments/assets/7eb2916b-c7fe-4ab7-a12d-fbd235c6905e" />
<img width="1828" height="503" alt="successful pipeline run - prod" src="https://github.com/user-attachments/assets/d067e76a-3b36-4753-aa14-22d4b830ec1c" />


## 3. Gold data

Run:

```sql
SELECT *
FROM telematics.prod.gold_truck_current
ORDER BY truck_id;
```

DEV:

<img width="1394" height="683" alt="gold current truck - dev" src="https://github.com/user-attachments/assets/19d37446-b030-406c-9cfa-6fba0e3fd609" />

TEST:

<img width="1412" height="736" alt="gold current truck - test" src="https://github.com/user-attachments/assets/affd66aa-ede4-453a-9e49-f03a09b82682" />

PROD:

<img width="1409" height="741" alt="gold current truck - prod" src="https://github.com/user-attachments/assets/8e7d9555-826e-41e1-838f-9f28dc90dcd7" />


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

TEST:

<img width="388" height="373" alt="silver dropping invalid rows - test" src="https://github.com/user-attachments/assets/ad5fce07-d25d-4ab5-bbd9-21397979e11a" />
<img width="1345" height="636" alt="silver removals - test" src="https://github.com/user-attachments/assets/0c5a27e5-ce4e-4ad1-8b69-ecd22c5944b8" />

PROD:

<img width="377" height="361" alt="silver dropping invalid rows - prod" src="https://github.com/user-attachments/assets/998ddcde-6577-48cf-b06d-f2d913e90c09" />
<img width="1347" height="786" alt="silver removals - prod" src="https://github.com/user-attachments/assets/ce7b58f9-72fe-477b-85fa-204e8a6924ba" />


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

TEST:

<img width="1061" height="512" alt="successful pre-migration run - test" src="https://github.com/user-attachments/assets/01db4c91-3e02-4f21-90c4-547ee85a7624" />
<img width="1066" height="505" alt="successful post-migration run - test" src="https://github.com/user-attachments/assets/80fae9d5-501e-4d6f-a559-27a100cfad74" />
<img width="1244" height="316" alt="migrations - test" src="https://github.com/user-attachments/assets/98f505d5-668b-48dc-9e92-0a62ba982d45" />

PROD:

<img width="899" height="579" alt="successful pre-migration run - prod" src="https://github.com/user-attachments/assets/a3cdfc6d-8a76-4550-8b5d-4ddf6b1f101f" />
<img width="904" height="578" alt="successful post-migration run - prod" src="https://github.com/user-attachments/assets/3354c0ff-2513-4747-b706-3dc0f8766d08" />
<img width="1244" height="319" alt="migrations - prod" src="https://github.com/user-attachments/assets/e2bb0aaa-a182-4bf3-89e0-c19d8e4aa481" />


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

DEV:

<img width="1691" height="497" alt="image" src="https://github.com/user-attachments/assets/344a58e2-3815-4f9a-b641-13cbb87cd45b" />
<img width="1330" height="309" alt="migration list - dev" src="https://github.com/user-attachments/assets/56e61d55-29ea-4a34-b2b2-bc9e18d782bf" />

<img width="2043" height="347" alt="isactive dropped streaming table - dev" src="https://github.com/user-attachments/assets/3ad7abbd-189c-45d1-a43a-e477f808dabf" />
<img width="1417" height="719" alt="isactive dropped materialized view - dev" src="https://github.com/user-attachments/assets/a3eeedf0-44b3-4fef-a953-a393965c5b2e" />
<img width="970" height="602" alt="isactive dropped static table - dev" src="https://github.com/user-attachments/assets/eba56bf9-be48-4ef8-b8ec-66a82ed76760" />

## 7. Optional schema-drift proof

Run prod with:

```bash
databricks bundle run -t prod --params drift_mode=rename_latitude,batches=3 telematics_orchestrator
```

Inspect recent Bronze rows and the pipeline expectation metrics for the `latitude` issue. Explain that Bronze tolerates
the drift, while curated contract changes require a reviewed code/migration change.

DEV:

<img width="1671" height="681" alt="schema change successful run - dev" src="https://github.com/user-attachments/assets/d703dfc2-7297-48b0-ae43-9fbb7ce0aaea" />

<img width="1478" height="588" alt="schema change rows bronze - dev" src="https://github.com/user-attachments/assets/dee52ebc-9ff3-4814-80f4-cd18255a9355" />
<img width="984" height="1194" alt="schema change cancelled run - dev" src="https://github.com/user-attachments/assets/54254ab9-1f2e-41b4-9c34-423442fa1510" />
<img width="977" height="1118" alt="schema change triggered run - dev" src="https://github.com/user-attachments/assets/00ca0c6e-2d3a-43a5-8f32-c7eb75645f2c" />
<img width="382" height="365" alt="schema change failed to promote rows due to invalid coordinates - dev" src="https://github.com/user-attachments/assets/1bdf603d-d46f-4709-9c17-b2e9999ab74d" />

