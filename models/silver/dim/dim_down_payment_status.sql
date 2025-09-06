-- models/silver/dim/dim_down_payment_status.sql
-- Silver layer down payment status dimension table reading from bronze sources

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS silver.finance.dim_down_payment_status;

CREATE TABLE silver.finance.dim_down_payment_status (
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

-- Insert down payment statuses from bronze.leaseend_db_public.financial_infos
INSERT INTO silver.finance.dim_down_payment_status (
  down_payment_status_key,
  down_payment_status_description,
  _source_table,
  _load_timestamp
)
WITH source_statuses AS (
  SELECT DISTINCT
    fi.down_payment_status,
    ROW_NUMBER() OVER (PARTITION BY fi.down_payment_status ORDER BY fi.down_payment_status) as rn
  FROM bronze.leaseend_db_public.financial_infos fi
  WHERE fi.down_payment_status IS NOT NULL 
    AND fi.down_payment_status <> ''
    AND (fi._fivetran_deleted = FALSE OR fi._fivetran_deleted IS NULL)
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
  'bronze.leaseend_db_public.financial_infos' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM source_statuses ss
WHERE ss.rn = 1

UNION ALL

-- Add standard unknown record
SELECT
  'Unknown' as down_payment_status_key,
  'Unknown' as down_payment_status_description,
  'system' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp; 