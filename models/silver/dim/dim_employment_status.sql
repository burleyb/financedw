-- models/silver/dim/dim_employment_status.sql
-- Silver layer employment status dimension table reading from bronze sources

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS silver.finance.dim_employment_status;

CREATE TABLE silver.finance.dim_employment_status (
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

-- Insert employment statuses from bronze.leaseend_db_public.employments
INSERT INTO silver.finance.dim_employment_status (
  employment_status_key,
  employment_status_description,
  _source_table,
  _load_timestamp
)
WITH source_statuses AS (
  SELECT DISTINCT
    e.status,
    ROW_NUMBER() OVER (PARTITION BY e.status ORDER BY e.status) as rn
  FROM bronze.leaseend_db_public.employments e
  WHERE e.status IS NOT NULL 
    AND e.status <> ''
    AND (e._fivetran_deleted = FALSE OR e._fivetran_deleted IS NULL)
)
SELECT
  ss.status AS employment_status_key,
  -- Map employment status to descriptive names
  CASE
    WHEN UPPER(ss.status) = 'EMPLOYED' THEN 'Employed'
    WHEN UPPER(ss.status) = 'UNEMPLOYED' THEN 'Unemployed'
    WHEN UPPER(ss.status) = 'SELF_EMPLOYED' THEN 'Self-Employed'
    WHEN UPPER(ss.status) = 'RETIRED' THEN 'Retired'
    WHEN UPPER(ss.status) = 'STUDENT' THEN 'Student'
    WHEN UPPER(ss.status) = 'DISABLED' THEN 'Disabled'
    ELSE COALESCE(ss.status, 'Unknown')
  END AS employment_status_description,
  'bronze.leaseend_db_public.employments' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM source_statuses ss
WHERE ss.rn = 1

UNION ALL

-- Add standard unknown record
SELECT
  'Unknown' as employment_status_key,
  'Unknown' as employment_status_description,
  'system' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp; 