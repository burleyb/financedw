-- models/dim/dim_lienholder.sql
-- Dimension table for lienholders

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_lienholder (
  lienholder_key STRING NOT NULL, -- Natural key preferred: lienholder_slug, fallback: lienholder_name
  lienholder_name STRING,
  lienholder_slug STRING,
  -- Add other relevant lienholder attributes if available (e.g., address)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct lienholders.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_lienholder AS target
USING (
  WITH source_lienholders AS (
      SELECT DISTINCT
          COALESCE(lienholder_slug, lienholder_name) as lh_key,
          lienholder_name as lh_name,
          lienholder_slug as lh_slug
      FROM silver.deal.big_deal
      WHERE COALESCE(lienholder_slug, lienholder_name) IS NOT NULL
        AND COALESCE(lienholder_slug, lienholder_name) <> ''
  )
  SELECT
    s.lh_key AS lienholder_key,
    s.lh_name AS lienholder_name,
    s.lh_slug AS lienholder_slug,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_lienholders s
  WHERE s.lh_key IS NOT NULL -- Redundant check, but safe
) AS source
ON target.lienholder_key = source.lienholder_key

-- Update existing lienholders if name or slug changes
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
  INSERT (lienholder_key, lienholder_name, lienholder_slug, _source_table, _load_timestamp)
  VALUES (source.lienholder_key, source.lienholder_name, source.lienholder_slug, source._source_table, CURRENT_TIMESTAMP());

-- Ensure 'Unknown' lienholder exists
MERGE INTO gold.finance.dim_lienholder AS target
USING (SELECT 'Unknown' as lienholder_key, 'Unknown' as lienholder_name, 'Unknown' as lienholder_slug, 'static' as _source_table) AS source
ON target.lienholder_key = source.lienholder_key
WHEN NOT MATCHED THEN INSERT (lienholder_key, lienholder_name, lienholder_slug, _source_table, _load_timestamp)
VALUES (source.lienholder_key, source.lienholder_name, source.lienholder_slug, source._source_table, current_timestamp()); 