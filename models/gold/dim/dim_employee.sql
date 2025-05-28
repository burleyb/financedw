-- models/gold/dim/dim_employee.sql
-- Gold layer employee dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_employee (
  employee_key STRING NOT NULL,
  employee_id INT,
  first_name STRING,
  last_name STRING,
  full_name STRING,
  email STRING,
  role STRING,
  department STRING,
  is_active BOOLEAN,
  hire_date DATE,
  termination_date DATE,
  employment_status STRING,
  manager_name STRING,
  location STRING,
  employee_display_name STRING,
  years_of_service DECIMAL(5,2),
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer employee dimension with business enhancements'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_employee AS target
USING (
  SELECT
    se.employee_key,
    se.employee_id,
    se.first_name,
    se.last_name,
    CONCAT(COALESCE(se.first_name, ''), ' ', COALESCE(se.last_name, '')) AS full_name,
    se.email,
    se.role,
    se.department,
    se.is_active,
    se.hire_date,
    se.termination_date,
    CASE 
      WHEN se.is_active = TRUE THEN 'Active'
      WHEN se.termination_date IS NOT NULL THEN 'Terminated'
      ELSE 'Unknown'
    END AS employment_status,
    se.manager_name,
    se.location,
    CASE 
      WHEN se.employee_key = 'Unknown' THEN 'Unknown Employee'
      ELSE CONCAT(COALESCE(se.first_name, ''), ' ', COALESCE(se.last_name, ''))
    END AS employee_display_name,
    CASE 
      WHEN se.hire_date IS NOT NULL THEN 
        ROUND(DATEDIFF(COALESCE(se.termination_date, CURRENT_DATE()), se.hire_date) / 365.25, 2)
      ELSE NULL
    END AS years_of_service,
    se._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_employee se
) AS source
ON target.employee_key = source.employee_key

WHEN MATCHED THEN
  UPDATE SET
    target.employee_id = source.employee_id,
    target.first_name = source.first_name,
    target.last_name = source.last_name,
    target.full_name = source.full_name,
    target.email = source.email,
    target.role = source.role,
    target.department = source.department,
    target.is_active = source.is_active,
    target.hire_date = source.hire_date,
    target.termination_date = source.termination_date,
    target.employment_status = source.employment_status,
    target.manager_name = source.manager_name,
    target.location = source.location,
    target.employee_display_name = source.employee_display_name,
    target.years_of_service = source.years_of_service,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    employee_key,
    employee_id,
    first_name,
    last_name,
    full_name,
    email,
    role,
    department,
    is_active,
    hire_date,
    termination_date,
    employment_status,
    manager_name,
    location,
    employee_display_name,
    years_of_service,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employee_key,
    source.employee_id,
    source.first_name,
    source.last_name,
    source.full_name,
    source.email,
    source.role,
    source.department,
    source.is_active,
    source.hire_date,
    source.termination_date,
    source.employment_status,
    source.manager_name,
    source.location,
    source.employee_display_name,
    source.years_of_service,
    source._source_table,
    source._load_timestamp
  ); 