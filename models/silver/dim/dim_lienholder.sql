-- models/silver/dim/dim_lienholder.sql
-- Silver layer lienholder dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_lienholder (
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

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_lienholder AS target
USING (
  WITH source_lienholders AS (
    SELECT DISTINCT
      d.lienholder_slug,
      d.lienholder_name,
      ROW_NUMBER() OVER (PARTITION BY COALESCE(d.lienholder_slug, d.lienholder_name) ORDER BY d.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.deals d
    WHERE COALESCE(d.lienholder_slug, d.lienholder_name) IS NOT NULL
      AND COALESCE(d.lienholder_slug, d.lienholder_name) <> ''
      AND d._fivetran_deleted = FALSE
  )
  SELECT
    COALESCE(sl.lienholder_slug, sl.lienholder_name) AS lienholder_key,
    sl.lienholder_name,
    sl.lienholder_slug,
    'bronze.leaseend_db_public.deals' AS _source_table
  FROM source_lienholders sl
  WHERE sl.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as lienholder_key,
    'Unknown' as lienholder_name,
    NULL as lienholder_slug,
    'system' AS _source_table
) AS source
ON target.lienholder_key = source.lienholder_key

-- Update existing lienholders if name changes
WHEN MATCHED AND (
    COALESCE(target.lienholder_name, '') <> COALESCE(source.lienholder_name, '') OR
    COALESCE(target.lienholder_slug, '') <> COALESCE(source.lienholder_slug, '')
  ) THEN
  UPDATE SET
    target.lienholder_name = source.lienholder_name,
    target.lienholder_slug = source.lienholder_slug,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new lienholders
WHEN NOT MATCHED THEN
  INSERT (
    lienholder_key,
    lienholder_name,
    lienholder_slug,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.lienholder_key,
    source.lienholder_name,
    source.lienholder_slug,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 