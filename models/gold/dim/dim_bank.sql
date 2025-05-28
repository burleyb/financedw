-- models/gold/dim/dim_bank.sql
-- Gold layer bank dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_bank (
  bank_key STRING NOT NULL,
  bank_name STRING,
  bank_slug STRING,
  bank_category STRING,
  is_active BOOLEAN,
  bank_display_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer bank dimension with business classifications'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_bank AS target
USING (
  SELECT
    sb.bank_key,
    sb.bank_name,
    sb.bank_slug,
    -- Categorize banks based on name patterns
    CASE
      WHEN UPPER(sb.bank_name) LIKE '%CREDIT UNION%' THEN 'Credit Union'
      WHEN UPPER(sb.bank_name) LIKE '%BANK%' THEN 'Bank'
      WHEN UPPER(sb.bank_name) LIKE '%FINANCIAL%' THEN 'Financial Services'
      WHEN sb.bank_name = 'Unknown' THEN 'Unknown'
      ELSE 'Other'
    END AS bank_category,
    CASE WHEN sb.bank_name = 'Unknown' THEN FALSE ELSE TRUE END AS is_active,
    COALESCE(sb.bank_name, 'Unknown Bank') AS bank_display_name,
    sb._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_bank sb
) AS source
ON target.bank_key = source.bank_key

WHEN MATCHED THEN
  UPDATE SET
    target.bank_name = source.bank_name,
    target.bank_slug = source.bank_slug,
    target.bank_category = source.bank_category,
    target.is_active = source.is_active,
    target.bank_display_name = source.bank_display_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    bank_key,
    bank_name,
    bank_slug,
    bank_category,
    is_active,
    bank_display_name,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.bank_key,
    source.bank_name,
    source.bank_slug,
    source.bank_category,
    source.is_active,
    source.bank_display_name,
    source._source_table,
    source._load_timestamp
  ); 