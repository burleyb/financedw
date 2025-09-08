-- models/gold/dim/dim_date.sql
-- Gold layer date dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_date;

CREATE TABLE gold.finance.dim_date (
  date_key INT NOT NULL,
  date_actual DATE,
  date_value DATE,
  `date` DATE,
  year INT,
  quarter INT,
  month INT,
  day_of_week INT,
  day_of_week_name STRING,
  day_of_month INT,
  day_of_year INT,
  week_of_year INT,
  month_actual INT,
  month_name STRING,
  month_name_short STRING,
  quarter_actual INT,
  quarter_name STRING,
  year_actual INT,
  fiscal_year INT,
  fiscal_quarter INT,
  fiscal_month INT,
  is_weekend BOOLEAN,
  is_holiday BOOLEAN,
  is_business_day BOOLEAN,
  days_from_today INT,
  week_start_date DATE,
  week_end_date DATE,
  month_start_date DATE,
  month_end_date DATE,
  quarter_start_date DATE,
  quarter_end_date DATE,
  year_start_date DATE,
  year_end_date DATE,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer date dimension with comprehensive business calendar attributes'
PARTITIONED BY (year_actual)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_date (
  date_key,
  date_actual,
  date_value,
  `date`,
  year,
  quarter,
  month,
  day_of_week,
  day_of_week_name,
  day_of_month,
  day_of_year,
  week_of_year,
  month_actual,
  month_name,
  month_name_short,
  quarter_actual,
  quarter_name,
  year_actual,
  fiscal_year,
  fiscal_quarter,
  fiscal_month,
  is_weekend,
  is_holiday,
  is_business_day,
  days_from_today,
  week_start_date,
  week_end_date,
  month_start_date,
  month_end_date,
  quarter_start_date,
  quarter_end_date,
  year_start_date,
  year_end_date,
  _source_table,
  _load_timestamp
)
SELECT
  sd.date_key,
  sd.date AS date_actual,
  sd.date AS date_value,
  sd.date AS `date`,
  sd.year,
  sd.quarter,
  sd.month,
  sd.day_of_week,
  sd.day_name AS day_of_week_name,
  sd.day_of_month,
  sd.day_of_year,
  sd.week_of_year,
  sd.month AS month_actual,
  sd.month_name,
  LEFT(sd.month_name, 3) AS month_name_short,
  sd.quarter AS quarter_actual,
  CONCAT('Q', sd.quarter) AS quarter_name,
  sd.year AS year_actual,
  sd.fiscal_year,
  sd.fiscal_quarter,
  sd.fiscal_month,
  sd.is_weekend,
  sd.is_holiday,
  CASE 
    WHEN sd.is_weekend = TRUE OR sd.is_holiday = TRUE THEN FALSE
    ELSE TRUE
  END AS is_business_day,
  CASE 
    WHEN sd.date IS NOT NULL THEN DATEDIFF(sd.date, CURRENT_DATE())
    ELSE NULL
  END AS days_from_today,
  CASE 
    WHEN sd.date IS NOT NULL THEN DATE_SUB(sd.date, sd.day_of_week - 1)
    ELSE NULL
  END AS week_start_date,
  CASE 
    WHEN sd.date IS NOT NULL THEN DATE_ADD(DATE_SUB(sd.date, sd.day_of_week - 1), 6)
    ELSE NULL
  END AS week_end_date,
  CASE 
    WHEN sd.date IS NOT NULL THEN DATE_TRUNC('MONTH', sd.date)
    ELSE NULL
  END AS month_start_date,
  CASE 
    WHEN sd.date IS NOT NULL THEN LAST_DAY(sd.date)
    ELSE NULL
  END AS month_end_date,
  CASE 
    WHEN sd.date IS NOT NULL THEN DATE_TRUNC('QUARTER', sd.date)
    ELSE NULL
  END AS quarter_start_date,
  CASE 
    WHEN sd.date IS NOT NULL THEN LAST_DAY(ADD_MONTHS(DATE_TRUNC('QUARTER', sd.date), 2))
    ELSE NULL
  END AS quarter_end_date,
  CASE 
    WHEN sd.date IS NOT NULL THEN DATE_TRUNC('YEAR', sd.date)
    ELSE NULL
  END AS year_start_date,
  CASE 
    WHEN sd.date IS NOT NULL AND sd.year IS NOT NULL THEN DATE(CONCAT(sd.year, '-12-31'))
    ELSE NULL
  END AS year_end_date,
  'silver.finance.dim_date' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_date sd; 