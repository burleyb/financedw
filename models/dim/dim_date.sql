-- models/dim/dim_date.sql

-- Creates or replaces the date dimension table.
-- This typically runs once or infrequently, generating a range of dates.

-- Determine the date range needed. Can be hardcoded or dynamically derived.
-- Example: Derive from deal creation/completion dates
-- SET VAR start_date = (SELECT CAST(MIN(creation_date_utc) AS DATE) FROM silver.deal.big_deal WHERE creation_date_utc IS NOT NULL);
-- SET VAR end_date = (SELECT CAST(MAX(completion_date_utc) AS DATE) FROM silver.deal.big_deal WHERE completion_date_utc IS NOT NULL);
-- Fallback if no deals exist yet:
-- SET VAR start_date = COALESCE(start_date, CAST('2020-01-01' AS DATE));
-- SET VAR end_date = COALESCE(end_date, CURRENT_DATE() + INTERVAL 1 YEAR); -- Project one year into the future

-- For simplicity in this example, let's use fixed dates.
-- Replace with dynamic dates using SET VAR above or pass as parameters.
-- SET VAR start_date = '1920-01-01';
-- SET VAR end_date = '2050-12-31';

CREATE OR REPLACE TABLE gold.finance.dim_date
USING DELTA
COMMENT 'Dimension table storing date attributes for analysis.'
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
  CASE WHEN DAYOFWEEK(ds.date_actual) IN (1, 7) THEN true ELSE false END AS is_weekend
  -- Add is_holiday flag based on a separate holiday table/logic if needed
FROM date_sequence ds; 