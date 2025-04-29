-- models/dim/dim_employment_status.sql
-- Dimension table for employment status

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS finance_gold.finance.dim_employment_status (
  employment_status_key STRING NOT NULL, -- Natural key from source
  employment_status_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct employment statuses.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO finance_gold.finance.dim_employment_status AS target
USING (
  WITH source_statuses AS (
      SELECT DISTINCT employment_status
      FROM silver.deal.big_deal
      WHERE employment_status IS NOT NULL AND employment_status <> ''
  )
  SELECT
    COALESCE(s.employment_status, 'unknown') AS employment_status_key,
    s.employment_status AS employment_status_description, -- Assuming status is descriptive
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_statuses s
) AS source
ON target.employment_status_key = source.employment_status_key

WHEN MATCHED AND target.employment_status_description <> source.employment_status_description THEN
  UPDATE SET
    target.employment_status_description = source.employment_status_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (employment_status_key, employment_status_description, _source_table, _load_timestamp)
  VALUES (source.employment_status_key, source.employment_status_description, source._source_table, CURRENT_TIMESTAMP());

-- Ensure 'Unknown' status exists
MERGE INTO finance_gold.finance.dim_employment_status AS target
USING (SELECT 'Unknown' as employment_status_key, 'Unknown' as employment_status_description, 'static' as _source_table) AS source
ON target.employment_status_key = source.employment_status_key
WHEN NOT MATCHED THEN INSERT (employment_status_key, employment_status_description, _source_table, _load_timestamp)
VALUES (source.employment_status_key, source.employment_status_description, source._source_table, current_timestamp()); 