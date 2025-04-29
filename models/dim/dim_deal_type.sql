-- models/dim/dim_deal_type.sql
-- Dimension table for deal type

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS finance_gold.finance.dim_deal_type (
  deal_type_key STRING NOT NULL, -- Natural key from source (e.g., type)
  deal_type_description STRING, -- Descriptive name for the deal type
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing distinct deal types and their attributes.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO finance_gold.finance.dim_deal_type AS target
USING (
  -- Source query: Select distinct deal types from source
  WITH source_types AS (
      SELECT DISTINCT type FROM silver.deal.big_deal WHERE type IS NOT NULL
  )
  SELECT
    s.type AS deal_type_key,
    -- Derive description based on known types
    CASE s.type
      WHEN 'acquisition' THEN 'Acquisition'
      WHEN 'refi' THEN 'Refinance'
      WHEN 'buyout' THEN 'Lease Buyout'
      ELSE COALESCE(s.type, 'Unknown') -- Handle potential future types
    END AS deal_type_description,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_types s
) AS source
ON target.deal_type_key = source.deal_type_key

-- Update existing types if description changes
WHEN MATCHED AND (
    target.deal_type_description <> source.deal_type_description
  ) THEN
  UPDATE SET
    target.deal_type_description = source.deal_type_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new types
WHEN NOT MATCHED THEN
  INSERT (
    deal_type_key,
    deal_type_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_type_key,
    source.deal_type_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- Optionally, ensure 'Unknown' type exists for handling NULLs during joins
MERGE INTO finance_gold.finance.dim_deal_type AS target
USING (SELECT 'Unknown' as deal_type_key, 'Unknown' as deal_type_description, 'static' as _source_table) AS source
ON target.deal_type_key = source.deal_type_key
WHEN NOT MATCHED THEN INSERT (deal_type_key, deal_type_description, _source_table, _load_timestamp)
VALUES (source.deal_type_key, source.deal_type_description, source._source_table, current_timestamp());