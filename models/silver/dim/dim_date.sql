-- models/silver/dim/dim_date.sql
-- Date dimension table for time-based analysis

-- Creates or replaces the date dimension table.
-- This typically runs once or infrequently, generating a range of dates.

CREATE OR REPLACE TABLE silver.finance.dim_date
USING DELTA
COMMENT 'Dimension table storing date attributes for analysis.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
AS
WITH date_sequence AS (
  -- Generate a sequence of dates within the desired range
  SELECT EXPLODE(SEQUENCE(CAST('1920-01-01' AS DATE), CAST('2050-12-31' AS DATE), INTERVAL 1 DAY)) AS date_actual
)
SELECT
  CAST(DATE_FORMAT(ds.date_actual, 'yyyyMMdd') AS INT) AS date_key, -- Surrogate key (e.g., 20230115)
  ds.date_actual AS date,
  YEAR(ds.date_actual) AS year,
  QUARTER(ds.date_actual) AS quarter,
  MONTH(ds.date_actual) AS month,
  DATE_FORMAT(ds.date_actual, 'MMMM') AS month_name,
  DAY(ds.date_actual) AS day_of_month,
  DAYOFWEEK(ds.date_actual) AS day_of_week, -- Sunday=1, Saturday=7
  DATE_FORMAT(ds.date_actual, 'EEEE') AS day_name,
  DAYOFYEAR(ds.date_actual) AS day_of_year,
  WEEKOFYEAR(ds.date_actual) AS week_of_year,
  -- Fiscal periods (example: fiscal year starts October)
  YEAR(ADD_MONTHS(ds.date_actual, 3)) AS fiscal_year,
  QUARTER(ADD_MONTHS(ds.date_actual, 3)) AS fiscal_quarter,
  MONTH(ADD_MONTHS(ds.date_actual, 3)) AS fiscal_month,
  -- Flags
  CASE WHEN DAYOFWEEK(ds.date_actual) IN (1, 7) THEN true ELSE false END AS is_weekend,
  -- Add is_holiday flag based on a separate holiday table/logic if needed
  false AS is_holiday
FROM date_sequence ds
UNION ALL
-- Add unknown/null date record
SELECT
  0 AS date_key,
  NULL AS date,
  NULL AS year,
  NULL AS quarter,
  NULL AS month,
  'Unknown' AS month_name,
  NULL AS day_of_month,
  NULL AS day_of_week,
  'Unknown' AS day_name,
  NULL AS day_of_year,
  NULL AS week_of_year,
  NULL AS fiscal_year,
  NULL AS fiscal_quarter,
  NULL AS fiscal_month,
  false AS is_weekend,
  false AS is_holiday; 