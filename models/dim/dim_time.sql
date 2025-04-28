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

CREATE OR REPLACE TABLE gold.finance.dim_time
USING DELTA
COMMENT 'Dimension table storing time attributes for analysis.'
AS
WITH time_sequence AS (
  -- Generate a sequence of timestamps for one day at 1-second intervals
  SELECT EXPLODE(SEQUENCE(
    CAST('2023-01-01 00:00:00' AS TIMESTAMP),
    CAST('2023-01-01 23:59:59' AS TIMESTAMP),
    INTERVAL 1 SECOND
  )) AS time_actual
)
SELECT
  CAST(DATE_FORMAT(ts.time_actual, 'HHmmss') AS INT) AS time_key, -- Surrogate key (e.g., 143022 for 14:30:22)
  ts.time_actual AS full_timestamp,
  HOUR(ts.time_actual) AS hour_of_day, -- 0 to 23
  MINUTE(ts.time_actual) AS minute_of_hour, -- 0 to 59
  SECOND(ts.time_actual) AS second_of_minute, -- 0 to 59
  DATE_FORMAT(ts.time_actual, 'HH:mm:ss') AS time_of_day, -- e.g., 14:30:22
  DATE_FORMAT(ts.time_actual, 'a') AS am_pm, -- AM or PM
  CASE 
    WHEN HOUR(ts.time_actual) < 12 THEN 'Morning'
    WHEN HOUR(ts.time_actual) < 17 THEN 'Afternoon'
    WHEN HOUR(ts.time_actual) < 20 THEN 'Evening'
    ELSE 'Night'
  END AS day_period,
  -- Flags
  CASE 
    WHEN HOUR(ts.time_actual) IN (9, 10, 11, 12, 13, 14, 15, 16, 17) THEN true 
    ELSE false 
  END AS is_work_hour, 
  CASE 
    WHEN SECOND(ts.time_actual) = 0 THEN true 
    ELSE false 
  END AS is_start_of_minute
FROM time_sequence ts;