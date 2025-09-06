-- models/gold/dim/dim_time.sql
-- Gold layer time dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_time;

CREATE TABLE gold.finance.dim_time (
  time_key INT NOT NULL,
  time_actual STRING NOT NULL,  -- Changed from TIME to STRING (Databricks doesn't support TIME)
  hour_24 INT,
  hour_12 INT,
  minute_actual INT,
  second_actual INT,
  am_pm STRING,
  hour_minute STRING,
  time_period STRING,
  is_business_hours BOOLEAN,
  shift_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer time dimension with business hour classifications'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_time (
  time_key,
  time_actual,
  hour_24,
  hour_12,
  minute_actual,
  second_actual,
  am_pm,
  hour_minute,
  time_period,
  is_business_hours,
  shift_name,
  _source_table,
  _load_timestamp
)
SELECT
  st.time_key,
  st.time_value AS time_actual,  -- Silver uses time_value (STRING), not time_actual
  st.hour_24,
  st.hour_12,  -- Silver already has hour_12 calculated
  st.minute AS minute_actual,  -- Silver uses 'minute', not 'minute_actual'
  st.second AS second_actual,  -- Silver uses 'second', not 'second_actual'
  st.am_pm,  -- Silver already has am_pm calculated
  CONCAT(LPAD(st.hour_24, 2, '0'), ':', LPAD(st.minute, 2, '0')) AS hour_minute,
  st.time_period,  -- Silver already has time_period calculated
  st.business_hours AS is_business_hours,  -- Silver uses 'business_hours', gold uses 'is_business_hours'
  CASE
    WHEN st.hour_24 BETWEEN 8 AND 16 THEN 'Day Shift'
    WHEN st.hour_24 BETWEEN 17 AND 23 THEN 'Evening Shift'
    ELSE 'Night Shift'
  END AS shift_name,
  st._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_time st; 