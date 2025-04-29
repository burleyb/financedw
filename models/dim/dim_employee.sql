-- models/dim/dim_employee.sql
-- Dimension table for employees, sourced primarily from HR data.

-- 1. Create the table if it doesn't exist (SCD Type 1 Example)
CREATE TABLE IF NOT EXISTS finance_gold.finance.dim_employee (
  employee_key STRING NOT NULL, -- Natural key from source system (e.g., BambooHR ID)
  first_name STRING,
  last_name STRING,
  full_name STRING,
  job_title STRING,
  department STRING,
  -- location STRING, -- Removed, not available in bronze.bamboohr.employee
  original_hire_date DATE,
  hire_date DATE,
  termination_date DATE,
  last_changed DATE,
  employment_status_date DATE,
  aca_status STRING,
  reporting_to STRING, -- Consider if this should be an employee_key FK later
  employment_status STRING, -- e.g., Active, Terminated
  state STRING,
  fte STRING, -- Consider DECIMAL/FLOAT if it's numeric
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing employee information from HR system.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
    -- Removed 'delta.columnMapping.mode' = 'name' as IDENTITY column is removed
);

-- Add constraint if employee_id_source should be unique (requires DBR 11.1+ or Unity Catalog)
-- ALTER TABLE gold.finance.dim_employee ADD CONSTRAINT dim_employee_id_unique UNIQUE (employee_key);

-- 2. Merge incremental changes from HR source
MERGE INTO finance_gold.finance.dim_employee AS target
USING (
  -- Source query: Select latest employee data from HR source
  -- Replace 'bronze.bamboohr.employee' with your actual cleaned HR table name
  -- Adjust column names (e.g., id, firstName, jobTitle) to match your source table
  SELECT
    CAST(id AS STRING) AS employee_key, -- Use natural key
    first_name,
    last_name,
    CONCAT_WS(' ', first_name, last_name) AS full_name, -- Construct full_name
    job_title,
    department,
    -- location, -- Removed
    CAST(original_hire_date AS DATE) AS original_hire_date,
    CAST(hire_date AS DATE) AS hire_date,
    CAST(termination_date AS DATE) AS termination_date,
    CAST(last_changed AS DATE) AS last_changed,
    CAST(employment_status_date AS DATE) AS employment_status_date,
    aca_status,
    reporting_to,
    employment_status,
    state,
    fte,
    'bronze.bamboohr.employee' AS _source_table -- Example source tracking
    -- Select other relevant attributes
  FROM bronze.bamboohr.employee -- <<< Replace with your HR source table
  WHERE id IS NOT NULL -- Ensure we have a valid natural key
  -- Select the most recent record if the source has history (e.g., based on an update timestamp)
  -- QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_synced DESC) = 1 -- Using _fivetran_synced as example ordering column
) AS source
-- Match on the natural key (source system ID)
ON target.employee_key = source.employee_key

-- Update existing employees if their data has changed (SCD Type 1)
WHEN MATCHED AND (
    -- Check for changes in relevant attributes
    target.first_name <> source.first_name OR
    target.last_name <> source.last_name OR
    target.full_name <> source.full_name OR
    target.job_title <> source.job_title OR
    target.department <> source.department OR
    -- target.location <> source.location OR -- Removed
    target.original_hire_date <> source.original_hire_date OR
    target.hire_date <> source.hire_date OR
    target.termination_date <> source.termination_date OR
    target.last_changed <> source.last_changed OR
    target.employment_status_date <> source.employment_status_date OR
    target.aca_status <> source.aca_status OR
    target.reporting_to <> source.reporting_to OR
    target.employment_status <> source.employment_status OR
    target.state <> source.state OR
    target.fte <> source.fte OR
    -- Handle NULL comparisons carefully for dates/nullable fields
    (target.termination_date IS NULL AND source.termination_date IS NOT NULL) OR (target.termination_date IS NOT NULL AND source.termination_date IS NULL) OR
    (target.original_hire_date IS NULL AND source.original_hire_date IS NOT NULL) OR (target.original_hire_date IS NOT NULL AND source.original_hire_date IS NULL)
    -- Add other NULL checks as needed
  ) THEN
  UPDATE SET
    target.first_name = source.first_name,
    target.last_name = source.last_name,
    target.full_name = source.full_name,
    target.job_title = source.job_title,
    target.department = source.department,
    -- target.location = source.location, -- Removed
    target.original_hire_date = source.original_hire_date,
    target.hire_date = source.hire_date,
    target.termination_date = source.termination_date,
    target.last_changed = source.last_changed,
    target.employment_status_date = source.employment_status_date,
    target.aca_status = source.aca_status,
    target.reporting_to = source.reporting_to,
    target.employment_status = source.employment_status,
    target.state = source.state,
    target.fte = source.fte,
    target._source_table = source._source_table,
    target._load_timestamp = current_timestamp()

-- Insert new employees (employee_key is the natural key)
WHEN NOT MATCHED THEN
  INSERT (
    employee_key,
    first_name,
    last_name,
    full_name,
    job_title,
    department,
    -- location,
    original_hire_date,
    hire_date,
    termination_date,
    last_changed,
    employment_status_date,
    aca_status,
    reporting_to,
    employment_status,
    state,
    fte,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employee_key,
    source.first_name,
    source.last_name,
    source.full_name,
    source.job_title,
    source.department,
    -- source.location,
    source.original_hire_date,
    source.hire_date,
    source.termination_date,
    source.last_changed,
    source.employment_status_date,
    source.aca_status,
    source.reporting_to,
    source.employment_status,
    source.state,
    source.fte,
    source._source_table,
    current_timestamp()
  ); 