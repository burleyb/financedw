-- models/silver/dim/dim_marital_status.sql
-- Silver layer marital status dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_marital_status (
  marital_status_key STRING NOT NULL, -- Natural key from source
  marital_status_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer marital status dimension storing distinct marital statuses'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_marital_status AS target
USING (
  WITH source_statuses AS (
    SELECT DISTINCT
      c.marital_status,
      ROW_NUMBER() OVER (PARTITION BY c.marital_status ORDER BY c.marital_status) as rn
    FROM bronze.leaseend_db_public.customers c
    WHERE c.marital_status IS NOT NULL 
      AND c.marital_status <> ''
      AND c._fivetran_deleted = FALSE
  )
  SELECT
    ss.marital_status AS marital_status_key,
    -- Map marital status to descriptive names
    CASE
      WHEN UPPER(ss.marital_status) = 'SINGLE' THEN 'Single'
      WHEN UPPER(ss.marital_status) = 'MARRIED' THEN 'Married'
      WHEN UPPER(ss.marital_status) = 'DIVORCED' THEN 'Divorced'
      WHEN UPPER(ss.marital_status) = 'WIDOWED' THEN 'Widowed'
      WHEN UPPER(ss.marital_status) = 'SEPARATED' THEN 'Separated'
      WHEN UPPER(ss.marital_status) = 'DOMESTIC_PARTNERSHIP' THEN 'Domestic Partnership'
      ELSE COALESCE(ss.marital_status, 'Unknown')
    END AS marital_status_description,
    'bronze.leaseend_db_public.customers' AS _source_table
  FROM source_statuses ss
  WHERE ss.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as marital_status_key,
    'Unknown' as marital_status_description,
    'system' AS _source_table
) AS source
ON target.marital_status_key = source.marital_status_key

-- Update existing statuses if description changes
WHEN MATCHED AND (
    target.marital_status_description <> source.marital_status_description
  ) THEN
  UPDATE SET
    target.marital_status_description = source.marital_status_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new statuses
WHEN NOT MATCHED THEN
  INSERT (
    marital_status_key,
    marital_status_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.marital_status_key,
    source.marital_status_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 