-- models/gold/dim/dim_employment.sql
-- Gold layer employment dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_employment (
  employment_key STRING NOT NULL,
  driver_key STRING,
  employer_name STRING,
  job_title STRING,
  employment_status_key STRING,
  pay_frequency_key STRING,
  monthly_income_cents BIGINT,
  monthly_income_dollars DECIMAL(15,2),
  years_employed DECIMAL(5,2),
  months_employed INT,
  employment_start_date DATE,
  employment_end_date DATE,
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

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_employment AS target
USING (
  SELECT
    se.employment_key,
    se.driver_key,
    se.employer_name,
    se.job_title,
    se.employment_status_key,
    se.pay_frequency_key,
    se.monthly_income_cents,
    ROUND(se.monthly_income_cents / 100.0, 2) AS monthly_income_dollars,
    se.years_employed,
    se.months_employed,
    se.employment_start_date,
    se.employment_end_date,
    se.is_current_employment,
    
    -- Income categorization
    CASE
      WHEN se.monthly_income_cents >= 1000000 THEN 'High Income ($10K+/month)'
      WHEN se.monthly_income_cents >= 500000 THEN 'Upper Middle ($5K-$10K/month)'
      WHEN se.monthly_income_cents >= 300000 THEN 'Middle Income ($3K-$5K/month)'
      WHEN se.monthly_income_cents >= 150000 THEN 'Lower Middle ($1.5K-$3K/month)'
      WHEN se.monthly_income_cents > 0 THEN 'Low Income (<$1.5K/month)'
      ELSE 'Unknown'
    END AS income_category,
    
    -- Employment stability assessment
    CASE
      WHEN se.years_employed >= 5 THEN 'Very Stable (5+ years)'
      WHEN se.years_employed >= 2 THEN 'Stable (2-5 years)'
      WHEN se.years_employed >= 1 THEN 'Moderate (1-2 years)'
      WHEN se.years_employed >= 0.5 THEN 'New (6-12 months)'
      WHEN se.years_employed > 0 THEN 'Very New (<6 months)'
      ELSE 'Unknown'
    END AS employment_stability,
    
    se._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_employment se
) AS source
ON target.employment_key = source.employment_key

WHEN MATCHED THEN
  UPDATE SET
    target.driver_key = source.driver_key,
    target.employer_name = source.employer_name,
    target.job_title = source.job_title,
    target.employment_status_key = source.employment_status_key,
    target.pay_frequency_key = source.pay_frequency_key,
    target.monthly_income_cents = source.monthly_income_cents,
    target.monthly_income_dollars = source.monthly_income_dollars,
    target.years_employed = source.years_employed,
    target.months_employed = source.months_employed,
    target.employment_start_date = source.employment_start_date,
    target.employment_end_date = source.employment_end_date,
    target.is_current_employment = source.is_current_employment,
    target.income_category = source.income_category,
    target.employment_stability = source.employment_stability,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    employment_key,
    driver_key,
    employer_name,
    job_title,
    employment_status_key,
    pay_frequency_key,
    monthly_income_cents,
    monthly_income_dollars,
    years_employed,
    months_employed,
    employment_start_date,
    employment_end_date,
    is_current_employment,
    income_category,
    employment_stability,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employment_key,
    source.driver_key,
    source.employer_name,
    source.job_title,
    source.employment_status_key,
    source.pay_frequency_key,
    source.monthly_income_cents,
    source.monthly_income_dollars,
    source.years_employed,
    source.months_employed,
    source.employment_start_date,
    source.employment_end_date,
    source.is_current_employment,
    source.income_category,
    source.employment_stability,
    source._source_table,
    source._load_timestamp
  ); 