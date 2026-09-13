MERGE INTO `{{catalog}}`.`{{schema}}`.`truck_details` AS target
USING (
  SELECT
    truck_id,
    CASE
      WHEN capacity_lbs >= 34000 THEN 'HEAVY'
      WHEN capacity_lbs >= 26000 THEN 'MEDIUM'
      ELSE 'LIGHT'
    END AS truck_class
  FROM `{{catalog}}`.`{{schema}}`.`truck_details`
  WHERE truck_class IS NULL
) AS source
ON target.truck_id = source.truck_id
WHEN MATCHED AND target.truck_class IS NULL THEN
  UPDATE SET target.truck_class = source.truck_class;
