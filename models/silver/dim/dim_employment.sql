-- models/silver/dim/dim_employment.sql
-- Silver layer employment dimension table reading from bronze sources
-- Implements SCD Type 2 for employment history tracking

CREATE TABLE IF NOT EXISTS silver.finance.dim_employment (
  employment_key INT NOT NULL, -- Natural key from source table (employments.id)
  driver_key STRING NOT NULL, -- Natural key from dim_driver (customer_id from source)
  employer STRING,
  job_title STRING,
  employment_type STRING,
  work_phone_number STRING,
  years_at_job SMALLINT,
  months_at_job SMALLINT,
  gross_income DECIMAL(11,2),
  pay_frequency STRING,
  employment_status STRING,

  -- SCD Type 2 Columns
  _effective_from_timestamp TIMESTAMP NOT NULL,
  _effective_to_timestamp TIMESTAMP,
  _is_current BOOLEAN NOT NULL,

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer employment dimension with SCD Type 2 history tracking'
PARTITIONED BY (employment_status)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Create temporary view for latest source data
CREATE OR REPLACE TEMPORARY VIEW latest_employment_source AS
SELECT
  id AS employment_key,
  CAST(customer_id AS STRING) AS driver_key,
  COALESCE(name, 'Unknown') AS employer,
  COALESCE(job_title, 'Unknown') AS job_title,
  COALESCE(employment_type, 'Unknown') AS employment_type,
  COALESCE(phone_number, 'Unknown') AS work_phone_number,
  COALESCE(years_at_job, 0) AS years_at_job,
  COALESCE(months_at_job, 0) AS months_at_job,
  COALESCE(gross_income, 0) AS gross_income,
  COALESCE(pay_frequency, 'Unknown') AS pay_frequency,
  COALESCE(status, 'Unknown') AS employment_status,
  updated_at
FROM bronze.leaseend_db_public.employments
WHERE id IS NOT NULL 
  AND customer_id IS NOT NULL
  AND _fivetran_deleted = FALSE
  -- Date validation
  AND updated_at >= '1900-01-01'
  AND updated_at <= '2037-12-31'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;

-- Create temporary view for changes detection
CREATE OR REPLACE TEMPORARY VIEW employment_changes AS
SELECT s.*
FROM latest_employment_source s
LEFT JOIN silver.finance.dim_employment t
    ON s.employment_key = t.employment_key AND t._is_current = TRUE
WHERE
    t.employment_key IS NULL -- New employment record
    OR (
        -- Check if any relevant attribute changed
        NOT (s.driver_key <=> t.driver_key AND
             s.employer <=> t.employer AND
             s.job_title <=> t.job_title AND
             s.employment_type <=> t.employment_type AND
             s.work_phone_number <=> t.work_phone_number AND
             s.years_at_job <=> t.years_at_job AND
             s.months_at_job <=> t.months_at_job AND
             s.gross_income <=> t.gross_income AND
             s.pay_frequency <=> t.pay_frequency AND
             s.employment_status <=> t.employment_status)
    );

-- Expire old records where changes are detected
MERGE INTO silver.finance.dim_employment AS target
USING employment_changes AS source
ON target.employment_key = source.employment_key AND target._is_current = TRUE
WHEN MATCHED THEN
  UPDATE SET
    target._is_current = FALSE,
    target._effective_to_timestamp = CURRENT_TIMESTAMP();

-- Insert new or updated records
INSERT INTO silver.finance.dim_employment (
    employment_key,
    driver_key,
    employer,
    job_title,
    employment_type,
    work_phone_number,
    years_at_job,
    months_at_job,
    gross_income,
    pay_frequency,
    employment_status,
    _effective_from_timestamp,
    _effective_to_timestamp,
    _is_current,
    _source_table,
    _load_timestamp
)
SELECT
    employment_key,
    driver_key,
    employer,
    job_title,
    employment_type,
    work_phone_number,
    years_at_job,
    months_at_job,
    gross_income,
    pay_frequency,
    employment_status,
    CURRENT_TIMESTAMP() AS _effective_from_timestamp,
    NULL AS _effective_to_timestamp,
    TRUE AS _is_current,
    'bronze.leaseend_db_public.employments' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
FROM employment_changes;

-- Clean up temporary views
DROP VIEW IF EXISTS latest_employment_source;
DROP VIEW IF EXISTS employment_changes; 