-- models/dim/dim_title_registration_option.sql
-- Dimension table for title and registration options

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS finance_gold.finance.dim_title_registration_option (
  title_registration_option_key STRING NOT NULL, -- Natural key from source (e.g., title_registration_option)
  title_registration_option_description STRING, -- Descriptive name for the option
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing distinct title and registration options.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO finance_gold.finance.dim_title_registration_option AS target
USING (
  -- Source query: Select distinct options from source
  WITH source_options AS (
      SELECT DISTINCT title_registration_option
      FROM silver.deal.big_deal
      WHERE title_registration_option IS NOT NULL
  )
  SELECT
    s.title_registration_option AS title_registration_option_key,
    -- Derive description based on known options
    CASE s.title_registration_option
      WHEN 'title_and_registration_transfer' THEN 'Title & Registration Transfer'
      WHEN 'title_only' THEN 'Title Only'
      WHEN 'title_and_new_registration' THEN 'Title & New Registration'
      ELSE COALESCE(s.title_registration_option, 'Unknown') -- Handle potential future options
    END AS title_registration_option_description,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_options s
) AS source
ON target.title_registration_option_key = source.title_registration_option_key

-- Update existing options if description changes
WHEN MATCHED AND (
    target.title_registration_option_description <> source.title_registration_option_description
  ) THEN
  UPDATE SET
    target.title_registration_option_description = source.title_registration_option_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new options
WHEN NOT MATCHED THEN
  INSERT (
    title_registration_option_key,
    title_registration_option_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.title_registration_option_key,
    source.title_registration_option_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- Ensure 'Unknown' option exists for handling NULLs
MERGE INTO finance_gold.finance.dim_title_registration_option AS target
USING (SELECT 'Unknown' as title_registration_option_key, 'Unknown' as title_registration_option_description, 'static' as _source_table) AS source
ON target.title_registration_option_key = source.title_registration_option_key
WHEN NOT MATCHED THEN INSERT (title_registration_option_key, title_registration_option_description, _source_table, _load_timestamp)
VALUES (source.title_registration_option_key, source.title_registration_option_description, source._source_table, current_timestamp()); 