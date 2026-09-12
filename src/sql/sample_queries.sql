-- Change the schema to dev, test, or prod as needed.
USE CATALOG telematics;
USE SCHEMA dev;

-- Business-ready current position output.
SELECT *
FROM gold_truck_current
ORDER BY truck_id;

-- Prove the stream-static integration exists at event grain.
SELECT truck_id, event_ts, latitude, longitude, driver, home_depot, region
FROM gold_pings_enriched
ORDER BY event_ts DESC
LIMIT 50;

-- Show migration history and checksum/commit traceability.
SELECT *
FROM _schema_migrations
ORDER BY applied_at;

-- Check rescued source drift.
SELECT
  COUNT(*) AS bronze_rows,
  COUNT_IF(_rescued_data IS NOT NULL) AS rescued_rows
FROM bronze_pings;

-- Verify expected deduplication at Silver grain.
SELECT truck_id, event_ts, latitude, longitude, COUNT(*) AS duplicate_count
FROM silver_pings
GROUP BY ALL
HAVING COUNT(*) > 1;
