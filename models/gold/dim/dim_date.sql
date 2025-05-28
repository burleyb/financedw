-- models/gold/dim/dim_date.sql
-- Gold layer date dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_date (
  date_key INT NOT NULL,
  date_actual DATE NOT NULL,
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

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_date AS target
USING (
  SELECT
    sd.date_key,
    sd.date_actual,
    sd.day_of_week,
    CASE sd.day_of_week
      WHEN 1 THEN 'Sunday'
      WHEN 2 THEN 'Monday'
      WHEN 3 THEN 'Tuesday'
      WHEN 4 THEN 'Wednesday'
      WHEN 5 THEN 'Thursday'
      WHEN 6 THEN 'Friday'
      WHEN 7 THEN 'Saturday'
    END AS day_of_week_name,
    sd.day_of_month,
    sd.day_of_year,
    WEEKOFYEAR(sd.date_actual) AS week_of_year,
    sd.month_actual,
    sd.month_name,
    LEFT(sd.month_name, 3) AS month_name_short,
    sd.quarter_actual,
    CONCAT('Q', sd.quarter_actual) AS quarter_name,
    sd.year_actual,
    sd.fiscal_year,
    sd.fiscal_quarter,
    sd.fiscal_month,
    sd.is_weekend,
    sd.is_holiday,
    CASE 
      WHEN sd.is_weekend = TRUE OR sd.is_holiday = TRUE THEN FALSE
      ELSE TRUE
    END AS is_business_day,
    DATEDIFF(sd.date_actual, CURRENT_DATE()) AS days_from_today,
    DATE_SUB(sd.date_actual, sd.day_of_week - 1) AS week_start_date,
    DATE_ADD(DATE_SUB(sd.date_actual, sd.day_of_week - 1), 6) AS week_end_date,
    DATE_TRUNC('MONTH', sd.date_actual) AS month_start_date,
    LAST_DAY(sd.date_actual) AS month_end_date,
    DATE_TRUNC('QUARTER', sd.date_actual) AS quarter_start_date,
    LAST_DAY(ADD_MONTHS(DATE_TRUNC('QUARTER', sd.date_actual), 2)) AS quarter_end_date,
    DATE_TRUNC('YEAR', sd.date_actual) AS year_start_date,
    DATE(CONCAT(sd.year_actual, '-12-31')) AS year_end_date,
    sd._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_date sd
) AS source
ON target.date_key = source.date_key

WHEN MATCHED THEN
  UPDATE SET
    target.date_actual = source.date_actual,
    target.day_of_week = source.day_of_week,
    target.day_of_week_name = source.day_of_week_name,
    target.day_of_month = source.day_of_month,
    target.day_of_year = source.day_of_year,
    target.week_of_year = source.week_of_year,
    target.month_actual = source.month_actual,
    target.month_name = source.month_name,
    target.month_name_short = source.month_name_short,
    target.quarter_actual = source.quarter_actual,
    target.quarter_name = source.quarter_name,
    target.year_actual = source.year_actual,
    target.fiscal_year = source.fiscal_year,
    target.fiscal_quarter = source.fiscal_quarter,
    target.fiscal_month = source.fiscal_month,
    target.is_weekend = source.is_weekend,
    target.is_holiday = source.is_holiday,
    target.is_business_day = source.is_business_day,
    target.days_from_today = source.days_from_today,
    target.week_start_date = source.week_start_date,
    target.week_end_date = source.week_end_date,
    target.month_start_date = source.month_start_date,
    target.month_end_date = source.month_end_date,
    target.quarter_start_date = source.quarter_start_date,
    target.quarter_end_date = source.quarter_end_date,
    target.year_start_date = source.year_start_date,
    target.year_end_date = source.year_end_date,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT *
  VALUES (
    source.date_key,
    source.date_actual,
    source.day_of_week,
    source.day_of_week_name,
    source.day_of_month,
    source.day_of_year,
    source.week_of_year,
    source.month_actual,
    source.month_name,
    source.month_name_short,
    source.quarter_actual,
    source.quarter_name,
    source.year_actual,
    source.fiscal_year,
    source.fiscal_quarter,
    source.fiscal_month,
    source.is_weekend,
    source.is_holiday,
    source.is_business_day,
    source.days_from_today,
    source.week_start_date,
    source.week_end_date,
    source.month_start_date,
    source.month_end_date,
    source.quarter_start_date,
    source.quarter_end_date,
    source.year_start_date,
    source.year_end_date,
    source._source_table,
    source._load_timestamp
  ); 