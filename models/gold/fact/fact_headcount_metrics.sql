-- models/gold/fact/fact_headcount_metrics.sql
-- Fact table for headcount metrics analysis

CREATE TABLE IF NOT EXISTS gold.finance.fact_headcount_metrics (
  date_key INT NOT NULL, -- Date in format YYYYMMDD
  department STRING NOT NULL,
  employment_status STRING NOT NULL,
  job_title STRING NOT NULL,
  location STRING NOT NULL,
  state STRING NOT NULL,
  -- Metrics
  headcount INT, -- Total employees active on this date
  new_hires INT, -- Employees hired on this date
  terminations INT, -- Employees terminated on this date
  net_change INT, -- New hires - terminations for this date
  -- Cumulative metrics
  ytd_hires INT, -- Year-to-date hires
  ytd_terminations INT, -- Year-to-date terminations
  ytd_net_change INT, -- Year-to-date net change
  -- Metadata
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Fact table for headcount metrics analysis'
PARTITIONED BY (date_key)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Generate daily headcount metrics
INSERT INTO gold.finance.fact_headcount_metrics
WITH date_range AS (
  SELECT date_key
  FROM gold.finance.dim_date
  WHERE date BETWEEN 
    (SELECT MIN(hire_date) FROM gold.finance.dim_employee WHERE hire_date IS NOT NULL) 
    AND CURRENT_DATE()
),
departments AS (
  SELECT DISTINCT COALESCE(department, 'Unknown') AS department
  FROM gold.finance.dim_employee
),
employment_statuses AS (
  SELECT DISTINCT COALESCE(employment_status, 'Unknown') AS employment_status
  FROM gold.finance.dim_employee
),
job_titles AS (
  SELECT DISTINCT COALESCE(job_title, 'Unknown') AS job_title
  FROM gold.finance.dim_employee
),
locations AS (
  SELECT DISTINCT COALESCE(location, 'Unknown') AS location
  FROM gold.finance.dim_employee
),
states AS (
  SELECT DISTINCT COALESCE(state, 'Unknown') AS state
  FROM gold.finance.dim_employee
),
dimensions AS (
  SELECT 
    d.date_key,
    dept.department,
    es.employment_status,
    jt.job_title,
    l.location,
    s.state
  FROM date_range d
  CROSS JOIN departments dept
  CROSS JOIN employment_statuses es
  CROSS JOIN job_titles jt
  CROSS JOIN locations l
  CROSS JOIN states s
),
daily_metrics AS (
  SELECT
    dim.date_key,
    dim.department,
    dim.employment_status,
    dim.job_title,
    dim.location,
    dim.state,
    -- Count active employees on this date
    COUNT(DISTINCT CASE 
      WHEN e.hire_date <= d.date 
        AND (e.termination_date IS NULL OR e.termination_date > d.date)
      THEN e.employee_key 
    END) AS headcount,
    -- Count new hires on this date
    COUNT(DISTINCT CASE 
      WHEN e.hire_date = d.date 
      THEN e.employee_key 
    END) AS new_hires,
    -- Count terminations on this date
    COUNT(DISTINCT CASE 
      WHEN e.termination_date = d.date 
      THEN e.employee_key 
    END) AS terminations,
    -- Net change
    COUNT(DISTINCT CASE 
      WHEN e.hire_date = d.date 
      THEN e.employee_key 
    END) - 
    COUNT(DISTINCT CASE 
      WHEN e.termination_date = d.date 
      THEN e.employee_key 
    END) AS net_change
  FROM dimensions dim
  JOIN gold.finance.dim_date d ON dim.date_key = d.date_key
  LEFT JOIN gold.finance.dim_employee e ON 
    (e.department = dim.department OR (e.department IS NULL AND dim.department = 'Unknown'))
    AND (e.employment_status = dim.employment_status OR (e.employment_status IS NULL AND dim.employment_status = 'Unknown'))
    AND (e.job_title = dim.job_title OR (e.job_title IS NULL AND dim.job_title = 'Unknown'))
    AND (e.location = dim.location OR (e.location IS NULL AND dim.location = 'Unknown'))
    AND (e.state = dim.state OR (e.state IS NULL AND dim.state = 'Unknown'))
  GROUP BY
    dim.date_key,
    dim.department,
    dim.employment_status,
    dim.job_title,
    dim.location,
    dim.state
),
ytd_metrics AS (
  SELECT
    dm.date_key,
    dm.department,
    dm.employment_status,
    dm.job_title,
    dm.location,
    dm.state,
    dm.headcount,
    dm.new_hires,
    dm.terminations,
    dm.net_change,
    -- Calculate year-to-date metrics
    SUM(dm2.new_hires) AS ytd_hires,
    SUM(dm2.terminations) AS ytd_terminations,
    SUM(dm2.net_change) AS ytd_net_change
  FROM daily_metrics dm
  JOIN gold.finance.dim_date d ON dm.date_key = d.date_key
  JOIN daily_metrics dm2 ON 
    dm2.department = dm.department
    AND dm2.employment_status = dm.employment_status
    AND dm2.job_title = dm.job_title
    AND dm2.location = dm.location
    AND dm2.state = dm.state
  JOIN gold.finance.dim_date d2 ON dm2.date_key = d2.date_key
  WHERE d2.year = d.year AND d2.date <= d.date
  GROUP BY
    dm.date_key,
    dm.department,
    dm.employment_status,
    dm.job_title,
    dm.location,
    dm.state,
    dm.headcount,
    dm.new_hires,
    dm.terminations,
    dm.net_change
)
SELECT
  date_key,
  department,
  employment_status,
  job_title,
  location,
  state,
  headcount,
  new_hires,
  terminations,
  net_change,
  ytd_hires,
  ytd_terminations,
  ytd_net_change,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM ytd_metrics;

