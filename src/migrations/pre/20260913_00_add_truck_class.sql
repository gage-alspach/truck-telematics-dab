BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM `{{catalog}}`.information_schema.columns
    WHERE table_schema = '{{schema}}'
      AND table_name = 'truck_details'
      AND column_name = 'truck_class'
  ) THEN
    ALTER TABLE `{{catalog}}`.`{{schema}}`.`truck_details`
      ADD COLUMN truck_class STRING COMMENT 'Operational truck class derived from rated capacity';
  END IF;
END;
