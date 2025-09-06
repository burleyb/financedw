-- models/gold/dim/dim_employment.sql
-- Gold layer employment dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_employment;

CREATE TABLE gold.finance.dim_employment (
  employment_key INT NOT NULL,
  driver_key STRING,
  employer_name STRING,
  job_title STRING,
  employment_type STRING,
  employment_status STRING,
  pay_frequency STRING,
  work_phone_number STRING,
  years_at_job SMALLINT,
  months_at_job SMALLINT,
  gross_income DECIMAL(11,2),
  gross_income_cents BIGINT,
  monthly_income_dollars DECIMAL(15,2),
  is_current_employment BOOLEAN,
  income_category STRING,
  employment_stability STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer employment dimension with income and stability analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_employment (
  employment_key,
  driver_key,
  employer_name,
  job_title,
  employment_type,
  employment_status,
  pay_frequency,
  work_phone_number,
  years_at_job,
  months_at_job,
  gross_income,
  gross_income_cents,
  monthly_income_dollars,
  is_current_employment,
  income_category,
  employment_stability,
  _source_table,
  _load_timestamp
)
SELECT
  se.employment_key,
  se.driver_key,
  se.employer AS employer_name,  -- Map employer to employer_name
  se.job_title,
  se.employment_type,
  se.employment_status,
  se.pay_frequency,
  se.work_phone_number,
  se.years_at_job,
  se.months_at_job,
  se.gross_income,
  CAST(se.gross_income * 100 AS BIGINT) AS gross_income_cents,  -- Convert to cents
  se.gross_income AS monthly_income_dollars,  -- Assuming gross_income is monthly
  se._is_current AS is_current_employment,
  
  -- Income categorization based on gross_income (assuming monthly)
  CASE
    WHEN se.gross_income >= 10000 THEN 'High Income ($10K+/month)'
    WHEN se.gross_income >= 5000 THEN 'Upper Middle ($5K-$10K/month)'
    WHEN se.gross_income >= 3000 THEN 'Middle Income ($3K-$5K/month)'
    WHEN se.gross_income >= 1500 THEN 'Lower Middle ($1.5K-$3K/month)'
    WHEN se.gross_income > 0 THEN 'Low Income (<$1.5K/month)'
    ELSE 'Unknown'
  END AS income_category,
  
  -- Employment stability assessment
  CASE
    WHEN se.years_at_job >= 5 THEN 'Very Stable (5+ years)'
    WHEN se.years_at_job >= 2 THEN 'Stable (2-5 years)'
    WHEN se.years_at_job >= 1 THEN 'Moderate (1-2 years)'
    WHEN se.years_at_job >= 0.5 THEN 'New (6-12 months)'
    WHEN se.years_at_job > 0 THEN 'Very New (<6 months)'
    ELSE 'Unknown'
  END AS employment_stability,
  
  se._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_employment se
WHERE se._is_current = TRUE;  -- Only get current employment records 