-- models/gold/dim/dim_bank.sql
-- Gold layer bank dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_bank;

CREATE TABLE gold.finance.dim_bank (
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

-- Insert all data from silver layer with business enhancements
INSERT INTO gold.finance.dim_bank (
  bank_key,
  bank_name,
  bank_slug,
  bank_category,
  is_active,
  bank_display_name,
  _source_table,
  _load_timestamp
)
SELECT
  source.bank_key,
  source.bank_name,
  source.bank_slug,
  source.bank_category,
  source.is_active,
  source.bank_display_name,
  source._source_table,
  source._load_timestamp
FROM (
  SELECT
    sb.bank_key,
    sb.bank_name,
    -- Generate bank_slug from bank_name by converting to lowercase and replacing spaces with hyphens
    LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(sb.bank_name), '[^a-zA-Z0-9\\s]', ''), '\\s+', '-')) AS bank_slug,
    -- Categorize banks based on name patterns
    CASE
      WHEN UPPER(sb.bank_name) LIKE '%CREDIT UNION%' THEN 'Credit Union'
      WHEN UPPER(sb.bank_name) LIKE '%BANK%' THEN 'Bank'
      WHEN UPPER(sb.bank_name) LIKE '%FINANCIAL%' THEN 'Financial Services'
      WHEN sb.bank_name = 'Unknown' THEN 'Unknown'
      ELSE 'Other'
    END AS bank_category,
    sb.is_active,
    COALESCE(sb.bank_display_name, sb.bank_name, 'Unknown Bank') AS bank_display_name,
    sb._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_bank sb
) AS source; 