-- models/dim/dim_marital_status.sql
-- Dimension table for marital status

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_marital_status (
  marital_status_key STRING NOT NULL, -- Natural key from source
  marital_status_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct marital statuses.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_marital_status AS target
USING (
  WITH source_statuses AS (
      SELECT DISTINCT marital_status
      FROM silver.deal.big_deal
      WHERE marital_status IS NOT NULL AND marital_status <> ''
  )
  SELECT
    s.marital_status AS marital_status_key,
    s.marital_status AS marital_status_description, -- Assuming status is descriptive
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_statuses s
) AS source
ON target.marital_status_key = source.marital_status_key

WHEN MATCHED AND target.marital_status_description <> source.marital_status_description THEN
  UPDATE SET
    target.marital_status_description = source.marital_status_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (marital_status_key, marital_status_description, _source_table, _load_timestamp)
  VALUES (source.marital_status_key, source.marital_status_description, source._source_table, CURRENT_TIMESTAMP());

-- Ensure 'Unknown' status exists
MERGE INTO gold.finance.dim_marital_status AS target
USING (SELECT 'Unknown' as marital_status_key, 'Unknown' as marital_status_description, 'static' as _source_table) AS source
ON target.marital_status_key = source.marital_status_key
WHEN NOT MATCHED THEN INSERT (marital_status_key, marital_status_description, _source_table, _load_timestamp)
VALUES (source.marital_status_key, source.marital_status_description, source._source_table, current_timestamp()); 