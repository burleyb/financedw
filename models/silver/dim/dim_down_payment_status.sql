-- models/silver/dim/dim_down_payment_status.sql
-- Silver layer down payment status dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_down_payment_status (
  down_payment_status_key STRING NOT NULL, -- Natural key from source
  down_payment_status_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer down payment status dimension storing distinct down payment statuses'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_down_payment_status AS target
USING (
  WITH source_statuses AS (
    SELECT DISTINCT
      d.down_payment_status,
      ROW_NUMBER() OVER (PARTITION BY d.down_payment_status ORDER BY d.down_payment_status) as rn
    FROM bronze.leaseend_db_public.deals d
    WHERE d.down_payment_status IS NOT NULL 
      AND d.down_payment_status <> ''
      AND d._fivetran_deleted = FALSE
  )
  SELECT
    ss.down_payment_status AS down_payment_status_key,
    -- Map down payment status to descriptive names
    CASE
      WHEN UPPER(ss.down_payment_status) = 'PAID' THEN 'Paid'
      WHEN UPPER(ss.down_payment_status) = 'PENDING' THEN 'Pending'
      WHEN UPPER(ss.down_payment_status) = 'NOT_REQUIRED' THEN 'Not Required'
      WHEN UPPER(ss.down_payment_status) = 'WAIVED' THEN 'Waived'
      WHEN UPPER(ss.down_payment_status) = 'PARTIAL' THEN 'Partial Payment'
      ELSE COALESCE(ss.down_payment_status, 'Unknown')
    END AS down_payment_status_description,
    'bronze.leaseend_db_public.deals' AS _source_table
  FROM source_statuses ss
  WHERE ss.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as down_payment_status_key,
    'Unknown' as down_payment_status_description,
    'system' AS _source_table
) AS source
ON target.down_payment_status_key = source.down_payment_status_key

-- Update existing statuses if description changes
WHEN MATCHED AND (
    target.down_payment_status_description <> source.down_payment_status_description
  ) THEN
  UPDATE SET
    target.down_payment_status_description = source.down_payment_status_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new statuses
WHEN NOT MATCHED THEN
  INSERT (
    down_payment_status_key,
    down_payment_status_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.down_payment_status_key,
    source.down_payment_status_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 