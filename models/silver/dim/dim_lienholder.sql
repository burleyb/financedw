-- models/silver/dim/dim_lienholder.sql
-- Silver layer lienholder dimension table reading from bronze sources

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS silver.finance.dim_lienholder;

CREATE TABLE silver.finance.dim_lienholder (
  lienholder_key STRING NOT NULL, -- Natural key: lienholder_slug or lienholder_name
  lienholder_name STRING,
  lienholder_slug STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer lienholder dimension storing distinct lienholders'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert lienholder data from bronze.leaseend_db_public.payoffs
INSERT INTO silver.finance.dim_lienholder (
  lienholder_key,
  lienholder_name,
  lienholder_slug,
  _source_table,
  _load_timestamp
)
WITH source_lienholders AS (
  SELECT DISTINCT
    p.lienholder_slug,
    p.lienholder_name,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(p.lienholder_slug, p.lienholder_name) ORDER BY p.updated_at DESC) as rn
  FROM bronze.leaseend_db_public.payoffs p
  WHERE COALESCE(p.lienholder_slug, p.lienholder_name) IS NOT NULL
    AND COALESCE(p.lienholder_slug, p.lienholder_name) <> ''
    AND (p._fivetran_deleted = FALSE OR p._fivetran_deleted IS NULL)
)
SELECT
  COALESCE(sl.lienholder_slug, sl.lienholder_name) AS lienholder_key,
  sl.lienholder_name,
  sl.lienholder_slug,
  'bronze.leaseend_db_public.payoffs' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM source_lienholders sl
WHERE sl.rn = 1

UNION ALL

-- Add standard unknown record
SELECT
  'Unknown' as lienholder_key,
  'Unknown' as lienholder_name,
  NULL as lienholder_slug,
  'system' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp; 