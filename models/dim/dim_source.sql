-- models/dim/dim_source.sql
-- Dimension table for deal sources

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_source (
  source_key STRING NOT NULL, -- Natural key from source (e.g., source column)
  source_description STRING, -- Descriptive name for the source
  source_name_detail STRING, -- Detailed source name if available (from source_name)
  other_source_detail STRING, -- Additional description if available (from other_source_description)
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing distinct deal sources and their attributes.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_source AS target
USING (
  -- Source query: Select distinct source combinations from source
  WITH source_combinations AS (
      SELECT DISTINCT
          source,
          source_name,
          other_source_description
      FROM silver.deal.big_deal
      WHERE source IS NOT NULL
  )
  SELECT
    s.source AS source_key,
    -- Derive description based on known source keys
    CASE s.source
        WHEN 'd2d' THEN 'Door-to-Door'
        WHEN 'outbound' THEN 'Outbound Call'
        WHEN 'call-in' THEN 'Inbound Call'
        WHEN 'web' THEN 'Web Lead'
        ELSE COALESCE(s.source_name, s.source, 'Unknown') -- Use source_name if available, else source key
    END AS source_description,
    s.source_name AS source_name_detail,
    s.other_source_description AS other_source_detail,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_combinations s
) AS source
ON target.source_key = source.source_key

-- Update existing sources if description or details change
WHEN MATCHED AND (
    target.source_description <> source.source_description OR
    COALESCE(target.source_name_detail, '') <> COALESCE(source.source_name_detail, '') OR
    COALESCE(target.other_source_detail, '') <> COALESCE(source.other_source_detail, '')
  ) THEN
  UPDATE SET
    target.source_description = source.source_description,
    target.source_name_detail = source.source_name_detail,
    target.other_source_detail = source.other_source_detail,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new sources
WHEN NOT MATCHED THEN
  INSERT (
    source_key,
    source_description,
    source_name_detail,
    other_source_detail,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.source_key,
    source.source_description,
    source.source_name_detail,
    source.other_source_detail,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- Ensure 'Unknown' source exists for handling NULLs
MERGE INTO gold.finance.dim_source AS target
USING (SELECT 'Unknown' as source_key, 'Unknown' as source_description, NULL as source_name_detail, NULL as other_source_detail, 'static' as _source_table) AS source
ON target.source_key = source.source_key
WHEN NOT MATCHED THEN INSERT (source_key, source_description, source_name_detail, other_source_detail, _source_table, _load_timestamp)
VALUES (source.source_key, source.source_description, source.source_name_detail, source.other_source_detail, source._source_table, current_timestamp());