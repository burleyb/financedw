-- models/dim/dim_employment.sql
-- Dimension table for employment details, using natural source ID as key (SCD Type 2).
-- Sourced from bronze.leaseend_db_public.employments

-- 1. Define Table Structure with History Tracking
CREATE TABLE IF NOT EXISTS gold.finance.dim_employment (
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
  _source_table STRING, -- Metadata: Originating table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing driver employment information with history (SCD Type 2), keyed by source employment ID.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
    -- Removed 'delta.columnMapping.mode' as IDENTITY column is removed
);

-- 2. Implement SCD Type 2 Logic using Temporary Views

-- Step 1: Create Temporary View for Latest Source Data (per employment record ID)
CREATE OR REPLACE TEMPORARY VIEW latest_source_data_temp_view AS
SELECT
  id AS employment_key, -- Use source id as the key
  CAST(customer_id AS STRING) AS driver_key,
  COALESCE(name, 'Unknown') AS employer,
  COALESCE(job_title, 'Unknown') AS job_title,
  COALESCE(employment_type, 'Unknown') AS employment_type,
  COALESCE(phone_number, 'Unknown') AS work_phone_number,
  COALESCE(years_at_job, 0) AS years_at_job,
  COALESCE(months_at_job, 0) AS months_at_job,
  COALESCE(gross_income, 0) AS gross_income,
  pay_frequency,
  status AS employment_status,
  updated_at -- Used for ordering within the same employment_key if needed, though likely unique
FROM bronze.leaseend_db_public.employments
WHERE id IS NOT NULL AND customer_id IS NOT NULL -- Ensure keys are not null
  AND _fivetran_deleted = FALSE
-- QUALIFY might not be strictly necessary if source `id` is truly unique and immutable,
-- but kept for safety if source could potentially update `id` records in place.
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;

-- Step 2: Create Temporary View for Changes (per employment record ID)
CREATE OR REPLACE TEMPORARY VIEW changes_temp_view AS
SELECT
    s.*
FROM latest_source_data_temp_view s
LEFT JOIN finance_gold.finance.dim_employment t
    ON s.employment_key = t.employment_key AND t._is_current = TRUE -- Join on employment_key now
WHERE
    t.employment_key IS NULL -- New employment record ID
    OR (
        -- Check if any relevant attribute changed for this specific employment record
        NOT (s.driver_key <=> t.driver_key AND -- Also check if driver link changed
             s.employer <=> t.employer AND
             s.job_title <=> t.job_title AND
             s.employment_type <=> t.employment_type AND
             s.work_phone_number <=> t.work_phone_number AND
             s.years_at_job <=> t.years_at_job AND
             s.months_at_job <=> t.months_at_job AND
             s.gross_income <=> t.gross_income AND
             s.pay_frequency <=> t.pay_frequency AND
             s.employment_status <=> t.employment_status)
    ); -- Existing employment record ID with changed attributes

-- Step 3: Expire old records where changes are detected.
-- Use the 'changes_temp_view' to find employment records needing expiration.
MERGE INTO gold.finance.dim_employment AS target
USING changes_temp_view AS source
ON target.employment_key = source.employment_key AND target._is_current = TRUE -- Match on employment_key
WHEN MATCHED THEN -- A change was detected for this employment_key, expire the existing current record
  UPDATE SET
    target._is_current = FALSE,
    target._effective_to_timestamp = CURRENT_TIMESTAMP();

-- Step 4: Insert the new or updated records identified in the 'changes_temp_view'.
INSERT INTO gold.finance.dim_employment (
    employment_key, -- Natural key from source
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
FROM changes_temp_view; -- Insert all records identified in the 'changes' view

-- Step 5: Clean up temporary views (optional but good practice)
-- DROP VIEW IF EXISTS latest_source_data_temp_view;
-- DROP VIEW IF EXISTS changes_temp_view;