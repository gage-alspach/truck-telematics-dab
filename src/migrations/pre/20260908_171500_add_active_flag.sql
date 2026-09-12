BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM `{{catalog}}`.information_schema.columns
    WHERE table_schema = '{{schema}}'
      AND table_name = 'truck_details'
      AND column_name = 'active_flag'
  ) THEN
    ALTER TABLE `{{catalog}}`.`{{schema}}`.`truck_details`
      ADD COLUMN active_flag BOOLEAN COMMENT 'Whether the truck is currently active';
  END IF;
END;
