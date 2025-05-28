-- models/silver/dim/dim_employment_status.sql
-- Silver layer employment status dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_employment_status (
  employment_status_key STRING NOT NULL, -- Natural key from source
  employment_status_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer employment status dimension storing distinct employment statuses'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_employment_status AS target
USING (
  WITH source_statuses AS (
    SELECT DISTINCT
      e.employment_status,
      ROW_NUMBER() OVER (PARTITION BY e.employment_status ORDER BY e.employment_status) as rn
    FROM bronze.leaseend_db_public.employments e
    WHERE e.employment_status IS NOT NULL 
      AND e.employment_status <> ''
      AND e._fivetran_deleted = FALSE
  )
  SELECT
    ss.employment_status AS employment_status_key,
    -- Map employment status to descriptive names
    CASE
      WHEN UPPER(ss.employment_status) = 'EMPLOYED' THEN 'Employed'
      WHEN UPPER(ss.employment_status) = 'UNEMPLOYED' THEN 'Unemployed'
      WHEN UPPER(ss.employment_status) = 'SELF_EMPLOYED' THEN 'Self-Employed'
      WHEN UPPER(ss.employment_status) = 'RETIRED' THEN 'Retired'
      WHEN UPPER(ss.employment_status) = 'STUDENT' THEN 'Student'
      WHEN UPPER(ss.employment_status) = 'DISABLED' THEN 'Disabled'
      ELSE COALESCE(ss.employment_status, 'Unknown')
    END AS employment_status_description,
    'bronze.leaseend_db_public.employments' AS _source_table
  FROM source_statuses ss
  WHERE ss.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as employment_status_key,
    'Unknown' as employment_status_description,
    'system' AS _source_table
) AS source
ON target.employment_status_key = source.employment_status_key

-- Update existing statuses if description changes
WHEN MATCHED AND (
    target.employment_status_description <> source.employment_status_description
  ) THEN
  UPDATE SET
    target.employment_status_description = source.employment_status_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new statuses
WHEN NOT MATCHED THEN
  INSERT (
    employment_status_key,
    employment_status_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employment_status_key,
    source.employment_status_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 