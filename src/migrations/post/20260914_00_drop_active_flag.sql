BEGIN
  IF EXISTS (
    SELECT 1
    FROM `{{catalog}}`.information_schema.columns
    WHERE table_schema = '{{schema}}'
      AND table_name = 'truck_details'
      AND column_name = 'active_flag'
  ) THEN
    ALTER TABLE `{{catalog}}`.`{{schema}}`.`truck_details`
      SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

    ALTER TABLE `{{catalog}}`.`{{schema}}`.`truck_details`
      DROP COLUMN active_flag;
  END IF;
END;
