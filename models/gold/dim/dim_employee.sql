-- models/gold/dim/dim_employee.sql
-- Gold layer employee dimension table with metrics-friendly fields

DROP TABLE IF EXISTS gold.finance.dim_employee;

CREATE TABLE IF NOT EXISTS gold.finance.dim_employee (
  employee_key STRING NOT NULL,
  employee_id INT,
  email STRING,
  name STRING,
  nickname STRING,
  original_hire_date_key INT, -- FK to dim_date
  hire_date_key INT, -- FK to dim_date
  termination_date_key INT, -- FK to dim_date
  created_date_key INT, -- FK to dim_date
  created_time_key INT, -- FK to dim_time
  updated_date_key INT, -- FK to dim_date
  updated_time_key INT, -- FK to dim_time
  is_active BOOLEAN,
  employment_status STRING,
  employment_status_date_key INT, -- FK to dim_date
  job_title STRING,
  department STRING,
  reporting_to STRING,
  location STRING,
  state STRING,
  fte STRING,
  -- Derived fields for metrics
  is_current_employee BOOLEAN, -- TRUE if active as of current date
  days_employed INT, -- Days from hire to termination or current date
  tenure_months INT, -- Months from hire to termination or current date
  tenure_years DECIMAL(5,2), -- Years from hire to termination or current date
  is_terminated BOOLEAN, -- TRUE if has termination date
  termination_year INT, -- Year of termination (NULL if active)
  termination_month INT, -- Month of termination (NULL if active)
  termination_quarter INT, -- Quarter of termination (NULL if active)
  hire_year INT, -- Year of hire
  hire_month INT, -- Month of hire
  hire_quarter INT, -- Quarter of hire
  -- Metadata fields
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer employee dimension with metrics-friendly fields for headcount analysis'
PARTITIONED BY (is_current_employee)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert/update logic for gold.finance.dim_employee
MERGE INTO gold.finance.dim_employee AS target
USING (
  WITH employee_dates AS (
    -- Join with date dimension to get date attributes for derived fields
    SELECT
      e.employee_key,
      e.employee_id,
      e.email,
      e.name,
      e.nickname,
      e.original_hire_date_key,
      e.hire_date_key,
      e.termination_date_key,
      e.created_date_key,
      e.created_time_key,
      e.updated_date_key,
      e.updated_time_key,
      e.is_active,
      e.employment_status,
      e.employment_status_date_key,
      e.job_title,
      e.department,
      e.reporting_to,
      e.location,
      e.state,
      e.fte,
      e._source_table,
      -- Get date values from date dimension for calculations
      hd.date AS hire_date,
      td.date AS termination_date,
      -- Extract attributes from date dimension
      hd.year AS hire_year,
      hd.month AS hire_month,
      hd.quarter AS hire_quarter,
      td.year AS termination_year,
      td.month AS termination_month,
      td.quarter AS termination_quarter
    FROM silver.finance.dim_employee e
    LEFT JOIN gold.finance.dim_date hd ON e.hire_date_key = hd.date_key
    LEFT JOIN gold.finance.dim_date td ON e.termination_date_key = td.date_key
    WHERE e.employee_key != 'unknown'
  )
  SELECT
    employee_key,
    employee_id,
    email,
    name,
    nickname,
    original_hire_date_key,
    hire_date_key,
    termination_date_key,
    created_date_key,
    created_time_key,
    updated_date_key,
    updated_time_key,
    is_active,
    employment_status,
    employment_status_date_key,
    job_title,
    department,
    reporting_to,
    location,
    state,
    fte,
    -- Derived fields for metrics
    CASE WHEN termination_date IS NULL OR termination_date > CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_current_employee,
    CASE 
      WHEN termination_date IS NOT NULL AND termination_date <= CURRENT_DATE() THEN DATEDIFF(termination_date, hire_date)
      ELSE DATEDIFF(CURRENT_DATE(), hire_date)
    END AS days_employed,
    CASE 
      WHEN termination_date IS NOT NULL AND termination_date <= CURRENT_DATE() THEN MONTHS_BETWEEN(termination_date, hire_date)
      ELSE MONTHS_BETWEEN(CURRENT_DATE(), hire_date)
    END AS tenure_months,
    CASE 
      WHEN termination_date IS NOT NULL AND termination_date <= CURRENT_DATE() THEN MONTHS_BETWEEN(termination_date, hire_date) / 12.0
      ELSE MONTHS_BETWEEN(CURRENT_DATE(), hire_date) / 12.0
    END AS tenure_years,
    CASE WHEN termination_date IS NOT NULL AND termination_date <= CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_terminated,
    termination_year,
    termination_month,
    termination_quarter,
    hire_year,
    hire_month,
    hire_quarter,
    _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM employee_dates
) AS source
ON target.employee_key = source.employee_key
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

-- Ensure 'Unknown' employee exists for handling NULLs
MERGE INTO gold.finance.dim_employee AS target
USING (
  SELECT 
    'unknown' as employee_key,
    NULL as employee_id,
    'Unknown' as email,
    'Unknown' as name,
    'Unknown' as nickname,
    NULL as original_hire_date_key,
    NULL as hire_date_key,
    NULL as termination_date_key,
    NULL as created_date_key,
    NULL as created_time_key,
    NULL as updated_date_key,
    NULL as updated_time_key,
    FALSE as is_active,
    'Unknown' as employment_status,
    NULL as employment_status_date_key,
    'Unknown' as job_title,
    'Unknown' as department,
    'Unknown' as reporting_to,
    'Unknown' as location,
    'Unknown' as state,
    NULL as fte,
    FALSE as is_current_employee,
    NULL as days_employed,
    NULL as tenure_months,
    NULL as tenure_years,
    FALSE as is_terminated,
    NULL as termination_year,
    NULL as termination_month,
    NULL as termination_quarter,
    NULL as hire_year,
    NULL as hire_month,
    NULL as hire_quarter,
    'system' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
) AS source
ON target.employee_key = source.employee_key
WHEN NOT MATCHED THEN 
  INSERT *; 