-- models/dim/dim_odometer_status.sql
-- Dimension table for odometer status

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_odometer_status (
  odometer_status_key STRING NOT NULL, -- Natural key from source
  odometer_status_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct odometer statuses.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_odometer_status AS target
USING (
  WITH source_statuses AS (
      SELECT DISTINCT odometer_status
      FROM silver.deal.big_deal
      WHERE odometer_status IS NOT NULL AND odometer_status <> ''
  )
  SELECT
    s.odometer_status AS odometer_status_key,
    s.odometer_status AS odometer_status_description, -- Assuming status is descriptive
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_statuses s
) AS source
ON target.odometer_status_key = source.odometer_status_key

WHEN MATCHED AND target.odometer_status_description <> source.odometer_status_description THEN
  UPDATE SET
    target.odometer_status_description = source.odometer_status_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (odometer_status_key, odometer_status_description, _source_table, _load_timestamp)
  VALUES (source.odometer_status_key, source.odometer_status_description, source._source_table, CURRENT_TIMESTAMP());

-- Ensure 'Unknown' status exists
MERGE INTO gold.finance.dim_odometer_status AS target
USING (SELECT 'Unknown' as odometer_status_key, 'Unknown' as odometer_status_description, 'static' as _source_table) AS source
ON target.odometer_status_key = source.odometer_status_key
WHEN NOT MATCHED THEN INSERT (odometer_status_key, odometer_status_description, _source_table, _load_timestamp)
VALUES (source.odometer_status_key, source.odometer_status_description, source._source_table, current_timestamp()); 