-- models/gold/fact/fact_headcount_metrics.sql
-- Fact table for headcount metrics analysis - INCREMENTAL LOAD

-- Create table if it doesn't exist
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

-- Incremental load: only process new/changed dates
MERGE INTO gold.finance.fact_headcount_metrics AS target
USING (
-- Get dates to process - full load on first run, incremental on subsequent runs
WITH table_exists AS (
  SELECT COUNT(*) > 0 AS has_data
  FROM gold.finance.fact_headcount_metrics
  LIMIT 1
),
process_dates AS (
  SELECT DISTINCT d.date_key, d.date_actual, d.year_actual
  FROM gold.finance.dim_date d
  CROSS JOIN table_exists te
  WHERE 
    CASE 
      -- First run: load all historical data
      WHEN NOT te.has_data THEN d.date_actual BETWEEN 
        (SELECT MIN(hd.date_actual) 
         FROM gold.finance.dim_employee e 
         JOIN gold.finance.dim_date hd ON e.hire_date_key = hd.date_key 
         WHERE e.hire_date_key IS NOT NULL) 
        AND CURRENT_DATE()
      -- Subsequent runs: incremental load
      ELSE d.date_actual >= CURRENT_DATE() - INTERVAL 7 DAYS
        OR d.date_key IN (
          -- Include dates with new hires
          SELECT e.hire_date_key
          FROM gold.finance.dim_employee e
          WHERE e.hire_date_key IS NOT NULL
          UNION
          -- Include dates with terminations
          SELECT e.termination_date_key
          FROM gold.finance.dim_employee e
          WHERE e.termination_date_key IS NOT NULL
        )
    END
),
-- Build metrics directly from employee data without CROSS JOIN
employee_daily_metrics AS (
  SELECT
    pd.date_key,
    COALESCE(e.department, 'Unknown') AS department,
    COALESCE(e.employment_status, 'Unknown') AS employment_status,
    COALESCE(e.job_title, 'Unknown') AS job_title,
    COALESCE(e.location, 'Unknown') AS location,
    COALESCE(e.state, 'Unknown') AS state,
    -- Count active employees on this date
    COUNT(DISTINCT CASE 
      WHEN hire_date.date_actual <= pd.date_actual 
        AND (term_date.date_actual IS NULL OR term_date.date_actual > pd.date_actual)
      THEN e.employee_key 
    END) AS headcount,
    -- Count new hires on this date
    COUNT(DISTINCT CASE 
      WHEN hire_date.date_actual = pd.date_actual 
      THEN e.employee_key 
    END) AS new_hires,
    -- Count terminations on this date
    COUNT(DISTINCT CASE 
      WHEN term_date.date_actual = pd.date_actual 
      THEN e.employee_key 
    END) AS terminations
  FROM process_dates pd
  CROSS JOIN gold.finance.dim_employee e
  LEFT JOIN gold.finance.dim_date hire_date ON e.hire_date_key = hire_date.date_key
  LEFT JOIN gold.finance.dim_date term_date ON e.termination_date_key = term_date.date_key
  WHERE e.employee_key != 'unknown' -- Exclude unknown records
  GROUP BY
    pd.date_key,
    pd.date_actual,
    pd.year_actual,
    COALESCE(e.department, 'Unknown'),
    COALESCE(e.employment_status, 'Unknown'),
    COALESCE(e.job_title, 'Unknown'),
    COALESCE(e.location, 'Unknown'),
    COALESCE(e.state, 'Unknown')
),
-- Calculate YTD metrics efficiently
final_metrics AS (
  SELECT
    edm.date_key,
    edm.department,
    edm.employment_status,
    edm.job_title,
    edm.location,
    edm.state,
    edm.headcount,
    edm.new_hires,
    edm.terminations,
    edm.new_hires - edm.terminations AS net_change,
    -- YTD calculations using window functions for efficiency
    SUM(edm.new_hires) OVER (
      PARTITION BY edm.department, edm.employment_status, edm.job_title, edm.location, edm.state, pd.year_actual 
      ORDER BY edm.date_key 
      ROWS UNBOUNDED PRECEDING
    ) AS ytd_hires,
    SUM(edm.terminations) OVER (
      PARTITION BY edm.department, edm.employment_status, edm.job_title, edm.location, edm.state, pd.year_actual 
      ORDER BY edm.date_key 
      ROWS UNBOUNDED PRECEDING
    ) AS ytd_terminations,
    SUM(edm.new_hires - edm.terminations) OVER (
      PARTITION BY edm.department, edm.employment_status, edm.job_title, edm.location, edm.state, pd.year_actual 
      ORDER BY edm.date_key 
      ROWS UNBOUNDED PRECEDING
    ) AS ytd_net_change
  FROM employee_daily_metrics edm
  JOIN process_dates pd ON edm.date_key = pd.date_key
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
FROM final_metrics
WHERE headcount > 0 OR new_hires > 0 OR terminations > 0 -- Only include rows with actual data
) AS source
ON target.date_key = source.date_key
  AND target.department = source.department
  AND target.employment_status = source.employment_status
  AND target.job_title = source.job_title
  AND target.location = source.location
  AND target.state = source.state
WHEN MATCHED THEN
  UPDATE SET
    headcount = source.headcount,
    new_hires = source.new_hires,
    terminations = source.terminations,
    net_change = source.net_change,
    ytd_hires = source.ytd_hires,
    ytd_terminations = source.ytd_terminations,
    ytd_net_change = source.ytd_net_change,
    _load_timestamp = source._load_timestamp
WHEN NOT MATCHED THEN
  INSERT (
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
    _load_timestamp
  )
  VALUES (
    source.date_key,
    source.department,
    source.employment_status,
    source.job_title,
    source.location,
    source.state,
    source.headcount,
    source.new_hires,
    source.terminations,
    source.net_change,
    source.ytd_hires,
    source.ytd_terminations,
    source.ytd_net_change,
    source._load_timestamp
  );