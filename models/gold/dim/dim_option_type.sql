-- models/gold/dim/dim_option_type.sql
-- Gold layer option type dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_option_type (
  option_type_key STRING NOT NULL,
  option_type_name STRING,
  has_vsc BOOLEAN,
  has_gap BOOLEAN,
  product_combination STRING,
  is_premium_package BOOLEAN,
  option_display_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer option type dimension with product combination analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_option_type AS target
USING (
  SELECT
    sot.option_type_key,
    sot.option_type_name,
    sot.has_vsc,
    sot.has_gap,
    -- Create business-friendly product combination names
    CASE
      WHEN sot.has_vsc = TRUE AND sot.has_gap = TRUE THEN 'VSC + GAP Bundle'
      WHEN sot.has_vsc = TRUE AND sot.has_gap = FALSE THEN 'VSC Only'
      WHEN sot.has_vsc = FALSE AND sot.has_gap = TRUE THEN 'GAP Only'
      WHEN sot.has_vsc = FALSE AND sot.has_gap = FALSE THEN 'No Protection'
      ELSE 'Unknown'
    END AS product_combination,
    -- Flag premium packages (both VSC and GAP)
    CASE WHEN sot.has_vsc = TRUE AND sot.has_gap = TRUE THEN TRUE ELSE FALSE END AS is_premium_package,
    -- Create display-friendly names
    CASE
      WHEN sot.option_type_key = 'Unknown' THEN 'Unknown Option'
      WHEN sot.has_vsc = TRUE AND sot.has_gap = TRUE THEN 'Complete Protection Package'
      WHEN sot.has_vsc = TRUE AND sot.has_gap = FALSE THEN 'Vehicle Service Contract'
      WHEN sot.has_vsc = FALSE AND sot.has_gap = TRUE THEN 'GAP Coverage'
      WHEN sot.has_vsc = FALSE AND sot.has_gap = FALSE THEN 'Basic Loan'
      ELSE COALESCE(sot.option_type_name, sot.option_type_key)
    END AS option_display_name,
    sot._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_option_type sot
) AS source
ON target.option_type_key = source.option_type_key

WHEN MATCHED THEN
  UPDATE SET
    target.option_type_name = source.option_type_name,
    target.has_vsc = source.has_vsc,
    target.has_gap = source.has_gap,
    target.product_combination = source.product_combination,
    target.is_premium_package = source.is_premium_package,
    target.option_display_name = source.option_display_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    option_type_key,
    option_type_name,
    has_vsc,
    has_gap,
    product_combination,
    is_premium_package,
    option_display_name,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.option_type_key,
    source.option_type_name,
    source.has_vsc,
    source.has_gap,
    source.product_combination,
    source.is_premium_package,
    source.option_display_name,
    source._source_table,
    source._load_timestamp
  ); 