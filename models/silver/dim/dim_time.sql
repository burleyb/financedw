-- models/silver/dim/dim_time.sql
-- Silver layer time dimension table with time-of-day breakdown

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS silver.finance.dim_time;

CREATE TABLE silver.finance.dim_time (
  time_key INT NOT NULL, -- HHMMSS format (e.g., 143022 for 14:30:22)
  time_value STRING, -- Actual time value in HH:MM:SS format
  hour_24 SMALLINT, -- 0-23
  hour_12 SMALLINT, -- 1-12
  minute SMALLINT, -- 0-59
  second SMALLINT, -- 0-59
  am_pm STRING, -- AM/PM
  hour_name STRING, -- "2:30 PM"
  time_period STRING, -- Morning, Afternoon, Evening, Night
  business_hours BOOLEAN, -- Is this during business hours (9 AM - 5 PM)?
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer time dimension with time-of-day attributes'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Generate time dimension data
INSERT OVERWRITE silver.finance.dim_time
SELECT
  -- Time key in HHMMSS format
  CAST(LPAD(hour_24, 2, '0') || LPAD(minute, 2, '0') || LPAD(second, 2, '0') AS INT) AS time_key,
  
  -- Time value in HH:MM:SS format
  LPAD(hour_24, 2, '0') || ':' || LPAD(minute, 2, '0') || ':' || LPAD(second, 2, '0') AS time_value,
  
  -- Time components
  hour_24,
  CASE WHEN hour_24 = 0 THEN 12
       WHEN hour_24 > 12 THEN hour_24 - 12
       ELSE hour_24 END AS hour_12,
  minute,
  second,
  
  -- AM/PM
  CASE WHEN hour_24 < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
  
  -- Hour name
  CASE WHEN hour_24 = 0 THEN '12:' || LPAD(minute, 2, '0') || ' AM'
       WHEN hour_24 < 12 THEN CAST(hour_24 AS STRING) || ':' || LPAD(minute, 2, '0') || ' AM'
       WHEN hour_24 = 12 THEN '12:' || LPAD(minute, 2, '0') || ' PM'
       ELSE CAST(hour_24 - 12 AS STRING) || ':' || LPAD(minute, 2, '0') || ' PM'
  END AS hour_name,
  
  -- Time period
  CASE WHEN hour_24 >= 5 AND hour_24 < 12 THEN 'Morning'
       WHEN hour_24 >= 12 AND hour_24 < 17 THEN 'Afternoon'
       WHEN hour_24 >= 17 AND hour_24 < 21 THEN 'Evening'
       ELSE 'Night'
  END AS time_period,
  
  -- Business hours (9 AM to 5 PM)
  CASE WHEN hour_24 >= 9 AND hour_24 < 17 THEN true ELSE false END AS business_hours,
  
  'generated' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp

FROM (
  -- Generate all possible time combinations (every minute for performance)
  SELECT 
    hour_seq.hour_24,
    minute_seq.minute,
    0 as second -- Only generate on the minute for performance
  FROM (
    SELECT EXPLODE(SEQUENCE(0, 23)) AS hour_24
  ) hour_seq
  CROSS JOIN (
    SELECT EXPLODE(SEQUENCE(0, 59)) AS minute
  ) minute_seq
)

UNION ALL

-- Add special time records
SELECT 
  0 AS time_key,
  '00:00:00' AS time_value,
  0 AS hour_24,
  12 AS hour_12,
  0 AS minute,
  0 AS second,
  'AM' AS am_pm,
  'Unknown' AS hour_name,
  'Unknown' AS time_period,
  false AS business_hours,
  'system' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp; 