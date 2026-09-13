CREATE TABLE IF NOT EXISTS `{{catalog}}`.`{{schema}}`.`truck_details` (
  truck_id STRING,
  make STRING,
  model STRING,
  capacity_lbs INT,
  home_depot STRING,
  region STRING,
  driver STRING
)
USING DELTA
COMMENT 'Static truck reference data seeded programmatically by the bundle';
