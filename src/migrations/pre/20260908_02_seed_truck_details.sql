-- Initial demo fleet. Preserve existing records on retries and previously seeded environments.
WITH fleet (truck_id, make, model, capacity_lbs, home_depot, region, driver, active_flag) AS (
  VALUES
    ('TRK-001', 'Freightliner', 'Cascadia', 34000, 'Chicago', 'Midwest', 'D. Patel', TRUE),
    ('TRK-002', 'Volvo', 'VNL', 20000, 'Dallas', 'South', 'I. Novak', TRUE),
    ('TRK-003', 'Freightliner', 'Cascadia', 20000, 'Atlanta', 'Southeast', 'A. Rivera', TRUE),
    ('TRK-004', 'Freightliner', 'Cascadia', 26000, 'Dallas', 'South', 'I. Novak', TRUE),
    ('TRK-005', 'Freightliner', 'Cascadia', 40000, 'Dallas', 'South', 'D. Patel', TRUE),
    ('TRK-006', 'Peterbilt', '579', 20000, 'Denver', 'West', 'C. Okafor', TRUE),
    ('TRK-007', 'Peterbilt', '579', 34000, 'Denver', 'West', 'C. Okafor', TRUE),
    ('TRK-008', 'Volvo', 'VNL', 20000, 'Denver', 'West', 'B. Chen', TRUE),
    ('TRK-009', 'Peterbilt', '579', 34000, 'Chicago', 'Midwest', 'F. Santos', TRUE),
    ('TRK-010', 'Kenworth', 'T680', 40000, 'Chicago', 'Midwest', 'I. Novak', TRUE),
    ('TRK-011', 'Freightliner', 'Cascadia', 20000, 'Atlanta', 'Southeast', 'I. Novak', TRUE),
    ('TRK-012', 'Kenworth', 'T680', 26000, 'Denver', 'West', 'B. Chen', TRUE),
    ('TRK-013', 'Freightliner', 'Cascadia', 34000, 'Dallas', 'South', 'B. Chen', TRUE),
    ('TRK-014', 'Volvo', 'VNL', 40000, 'Chicago', 'Midwest', 'E. Nguyen', TRUE),
    ('TRK-015', 'Peterbilt', '579', 26000, 'Denver', 'West', 'F. Santos', TRUE),
    ('TRK-016', 'Kenworth', 'T680', 34000, 'Dallas', 'South', 'B. Chen', TRUE),
    ('TRK-017', 'Volvo', 'VNL', 26000, 'Dallas', 'South', 'H. Brooks', TRUE),
    ('TRK-018', 'Peterbilt', '579', 26000, 'Denver', 'West', 'F. Santos', TRUE),
    ('TRK-019', 'Freightliner', 'Cascadia', 20000, 'Dallas', 'South', 'F. Santos', TRUE),
    ('TRK-020', 'Peterbilt', '579', 20000, 'Denver', 'West', 'D. Patel', TRUE)
)
MERGE INTO `{{catalog}}`.`{{schema}}`.`truck_details` AS t
USING fleet AS s
ON t.truck_id = s.truck_id
WHEN NOT MATCHED THEN INSERT (truck_id, make, model, capacity_lbs, home_depot, region, driver, active_flag)
VALUES (s.truck_id, s.make, s.model, s.capacity_lbs, s.home_depot, s.region, s.driver, s.active_flag);
