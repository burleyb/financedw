-- models/dim/dim_residence_type.sql
-- Dimension table for residence type

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_residence_type (
  residence_type_key STRING NOT NULL, -- Natural key from source
  residence_type_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct residence types.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_residence_type AS target
USING (
  WITH source_types AS (
      SELECT DISTINCT residence_type
      FROM silver.deal.big_deal
      WHERE residence_type IS NOT NULL AND residence_type <> ''
  )
  SELECT
    s.residence_type AS residence_type_key,
    s.residence_type AS residence_type_description, -- Assuming type is descriptive
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_types s
) AS source
ON target.residence_type_key = source.residence_type_key

WHEN MATCHED AND target.residence_type_description <> source.residence_type_description THEN
  UPDATE SET
    target.residence_type_description = source.residence_type_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (residence_type_key, residence_type_description, _source_table, _load_timestamp)
  VALUES (source.residence_type_key, source.residence_type_description, source._source_table, CURRENT_TIMESTAMP());

-- Ensure 'Unknown' type exists
MERGE INTO gold.finance.dim_residence_type AS target
USING (SELECT 'Unknown' as residence_type_key, 'Unknown' as residence_type_description, 'static' as _source_table) AS source
ON target.residence_type_key = source.residence_type_key
WHEN NOT MATCHED THEN INSERT (residence_type_key, residence_type_description, _source_table, _load_timestamp)
VALUES (source.residence_type_key, source.residence_type_description, source._source_table, current_timestamp()); 