-- models/dim/dim_pay_frequency.sql
-- Dimension table for pay frequency

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_pay_frequency (
  pay_frequency_key STRING NOT NULL, -- Natural key from source
  pay_frequency_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct pay frequencies.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_pay_frequency AS target
USING (
  WITH source_frequencies AS (
      SELECT DISTINCT pay_frequency
      FROM silver.deal.big_deal
      WHERE pay_frequency IS NOT NULL AND pay_frequency <> ''
  )
  SELECT
    s.pay_frequency AS pay_frequency_key,
    s.pay_frequency AS pay_frequency_description, -- Assuming frequency is descriptive
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_frequencies s
) AS source
ON target.pay_frequency_key = source.pay_frequency_key

WHEN MATCHED AND target.pay_frequency_description <> source.pay_frequency_description THEN
  UPDATE SET
    target.pay_frequency_description = source.pay_frequency_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (pay_frequency_key, pay_frequency_description, _source_table, _load_timestamp)
  VALUES (source.pay_frequency_key, source.pay_frequency_description, source._source_table, CURRENT_TIMESTAMP());

-- Ensure 'Unknown' frequency exists
MERGE INTO gold.finance.dim_pay_frequency AS target
USING (SELECT 'Unknown' as pay_frequency_key, 'Unknown' as pay_frequency_description, 'static' as _source_table) AS source
ON target.pay_frequency_key = source.pay_frequency_key
WHEN NOT MATCHED THEN INSERT (pay_frequency_key, pay_frequency_description, _source_table, _load_timestamp)
VALUES (source.pay_frequency_key, source.pay_frequency_description, source._source_table, current_timestamp()); 