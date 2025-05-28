-- models/silver/dim/dim_odometer_status.sql
-- Silver layer odometer status dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_odometer_status (
  odometer_status_key STRING NOT NULL, -- Natural key from source
  odometer_status_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer odometer status dimension storing distinct odometer reading statuses'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_odometer_status AS target
USING (
  WITH source_statuses AS (
    SELECT DISTINCT
      c.odometer_status,
      ROW_NUMBER() OVER (PARTITION BY c.odometer_status ORDER BY c.odometer_status) as rn
    FROM bronze.leaseend_db_public.cars c
    WHERE c.odometer_status IS NOT NULL 
      AND c.odometer_status <> ''
      AND c._fivetran_deleted = FALSE
  )
  SELECT
    ss.odometer_status AS odometer_status_key,
    -- Map odometer status to descriptive names
    CASE
      WHEN UPPER(ss.odometer_status) = 'ACTUAL' THEN 'Actual Mileage'
      WHEN UPPER(ss.odometer_status) = 'EXEMPT' THEN 'Exempt from Odometer Disclosure'
      WHEN UPPER(ss.odometer_status) = 'NOT_ACTUAL' THEN 'Not Actual Mileage'
      WHEN UPPER(ss.odometer_status) = 'EXCEEDS_LIMIT' THEN 'Exceeds Mechanical Limits'
      WHEN UPPER(ss.odometer_status) = 'UNKNOWN' THEN 'Unknown'
      ELSE COALESCE(ss.odometer_status, 'Unknown')
    END AS odometer_status_description,
    'bronze.leaseend_db_public.cars' AS _source_table
  FROM source_statuses ss
  WHERE ss.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as odometer_status_key,
    'Unknown' as odometer_status_description,
    'system' AS _source_table
) AS source
ON target.odometer_status_key = source.odometer_status_key

-- Update existing statuses if description changes
WHEN MATCHED AND (
    target.odometer_status_description <> source.odometer_status_description
  ) THEN
  UPDATE SET
    target.odometer_status_description = source.odometer_status_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new statuses
WHEN NOT MATCHED THEN
  INSERT (
    odometer_status_key,
    odometer_status_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.odometer_status_key,
    source.odometer_status_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 