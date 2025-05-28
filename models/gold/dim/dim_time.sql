-- models/gold/dim/dim_time.sql
-- Gold layer time dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_time (
  time_key INT NOT NULL,
  time_actual TIME NOT NULL,
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

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_time AS target
USING (
  SELECT
    st.time_key,
    st.time_actual,
    st.hour_24,
    CASE 
      WHEN st.hour_24 = 0 THEN 12
      WHEN st.hour_24 > 12 THEN st.hour_24 - 12
      ELSE st.hour_24
    END AS hour_12,
    st.minute_actual,
    st.second_actual,
    CASE WHEN st.hour_24 < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
    CONCAT(LPAD(st.hour_24, 2, '0'), ':', LPAD(st.minute_actual, 2, '0')) AS hour_minute,
    CASE
      WHEN st.hour_24 BETWEEN 6 AND 11 THEN 'Morning'
      WHEN st.hour_24 BETWEEN 12 AND 17 THEN 'Afternoon'
      WHEN st.hour_24 BETWEEN 18 AND 21 THEN 'Evening'
      ELSE 'Night'
    END AS time_period,
    st.is_business_hours,
    CASE
      WHEN st.hour_24 BETWEEN 8 AND 16 THEN 'Day Shift'
      WHEN st.hour_24 BETWEEN 17 AND 23 THEN 'Evening Shift'
      ELSE 'Night Shift'
    END AS shift_name,
    st._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_time st
) AS source
ON target.time_key = source.time_key

WHEN MATCHED THEN
  UPDATE SET
    target.time_actual = source.time_actual,
    target.hour_24 = source.hour_24,
    target.hour_12 = source.hour_12,
    target.minute_actual = source.minute_actual,
    target.second_actual = source.second_actual,
    target.am_pm = source.am_pm,
    target.hour_minute = source.hour_minute,
    target.time_period = source.time_period,
    target.is_business_hours = source.is_business_hours,
    target.shift_name = source.shift_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
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
  VALUES (
    source.time_key,
    source.time_actual,
    source.hour_24,
    source.hour_12,
    source.minute_actual,
    source.second_actual,
    source.am_pm,
    source.hour_minute,
    source.time_period,
    source.is_business_hours,
    source.shift_name,
    source._source_table,
    source._load_timestamp
  ); 