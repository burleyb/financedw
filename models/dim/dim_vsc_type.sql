-- models/dim/dim_vsc_type.sql
-- Dimension table for VSC (Vehicle Service Contract) types

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_vsc_type (
  vsc_type_key STRING NOT NULL, -- Natural key from source (e.g., vsc_type)
  vsc_type_description STRING, -- Descriptive name for the VSC type
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing distinct VSC types.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_vsc_type AS target
USING (
  -- Source query: Select distinct VSC types from source
  WITH source_types AS (
      SELECT DISTINCT vsc_type
      FROM silver.deal.big_deal
      WHERE vsc_type IS NOT NULL AND vsc_type <> '' -- Exclude NULLs and empty strings
  )
  SELECT
    s.vsc_type AS vsc_type_key,
    -- Assuming the type itself is descriptive enough, just use it
    -- Add specific CASE statement here if more descriptive names are needed
    CASE
      WHEN s.vsc_type = 'VALUE' THEN 'Value'
      WHEN s.vsc_type = 'BASIC' THEN 'Basic'
      WHEN s.vsc_type = 'MAJOR' THEN 'Major'
      ELSE COALESCE(s.vsc_type, 'Unknown') -- Handle potential future options
    END AS vsc_type_description,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_types s
) AS source
ON target.vsc_type_key = source.vsc_type_key

-- Update existing types if description changes (might be redundant if description = key)
WHEN MATCHED AND (
    target.vsc_type_description <> source.vsc_type_description
  ) THEN
  UPDATE SET
    target.vsc_type_description = source.vsc_type_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new types
WHEN NOT MATCHED THEN
  INSERT (
    vsc_type_key,
    vsc_type_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.vsc_type_key,
    source.vsc_type_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- Ensure 'Unknown' type exists for handling NULLs or empty strings
MERGE INTO gold.finance.dim_vsc_type AS target
USING (SELECT 'Unknown' as vsc_type_key, 'Unknown' as vsc_type_description, 'static' as _source_table) AS source
ON target.vsc_type_key = source.vsc_type_key
WHEN NOT MATCHED THEN INSERT (vsc_type_key, vsc_type_description, _source_table, _load_timestamp)
VALUES (source.vsc_type_key, source.vsc_type_description, source._source_table, current_timestamp());
