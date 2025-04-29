-- models/dim/dim_option_type.sql
-- Dimension table for deal option types (VSC, GAP, etc.)

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS finance_gold.finance.dim_option_type (
  option_type_key STRING NOT NULL, -- Natural key from source (e.g., option_type)
  option_type_description STRING, -- Descriptive name for the option type
  includes_vsc BOOLEAN, -- Flag indicating if VSC is included
  includes_gap BOOLEAN, -- Flag indicating if GAP is included
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing distinct deal option types (VSC, GAP) and their attributes.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO finance_gold.finance.dim_option_type AS target
USING (
  -- Source query: Select distinct option types from source
  WITH source_options AS (
      SELECT DISTINCT option_type FROM silver.deal.big_deal WHERE option_type IS NOT NULL
  )
  SELECT
    s.option_type AS option_type_key,
    -- Derive description based on known types
    CASE s.option_type
      WHEN 'vscPlusGap' THEN 'VSC + GAP'
      WHEN 'vsc' THEN 'VSC Only'
      WHEN 'gap' THEN 'GAP Only'
      WHEN 'noProducts' THEN 'No Products'
      ELSE COALESCE(s.option_type, 'Unknown') -- Handle potential future types
    END AS option_type_description,
    -- Derive flags based on type key
    CASE
      WHEN s.option_type IN ('vscPlusGap', 'vsc') THEN true
      ELSE false
    END AS includes_vsc,
    CASE
      WHEN s.option_type IN ('vscPlusGap', 'gap') THEN true
      ELSE false
    END AS includes_gap,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_options s
) AS source
ON target.option_type_key = source.option_type_key

-- Update existing types if description or flags change
WHEN MATCHED AND (
    target.option_type_description <> source.option_type_description OR
    target.includes_vsc <> source.includes_vsc OR
    target.includes_gap <> source.includes_gap
  ) THEN
  UPDATE SET
    target.option_type_description = source.option_type_description,
    target.includes_vsc = source.includes_vsc,
    target.includes_gap = source.includes_gap,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new types
WHEN NOT MATCHED THEN
  INSERT (
    option_type_key,
    option_type_description,
    includes_vsc,
    includes_gap,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.option_type_key,
    source.option_type_description,
    source.includes_vsc,
    source.includes_gap,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- Ensure 'Unknown' type exists for handling NULLs
MERGE INTO finance_gold.finance.dim_option_type AS target
USING (SELECT 'Unknown' as option_type_key, 'Unknown' as option_type_description, false as includes_vsc, false as includes_gap, 'static' as _source_table) AS source
ON target.option_type_key = source.option_type_key
WHEN NOT MATCHED THEN INSERT (option_type_key, option_type_description, includes_vsc, includes_gap, _source_table, _load_timestamp)
VALUES (source.option_type_key, source.option_type_description, source.includes_vsc, source.includes_gap, source._source_table, current_timestamp());
