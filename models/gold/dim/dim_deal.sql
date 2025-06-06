-- models/gold/dim/dim_deal.sql
DROP TABLE gold.finance.dim_deal;
-- Gold layer dimension table sourcing from silver layer
-- This table provides business-ready, optimized deal attributes for analytics

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_deal (
  deal_key STRING NOT NULL, -- Natural key from source (deal.id)
  -- Foreign Keys to other dimensions (will be populated correctly in fact tables)
  deal_state_key STRING,
  deal_type_key STRING,
  source_key STRING,
  option_type_key STRING,
  title_registration_option_key STRING,
  processor_key STRING, -- Renamed from employee_processor_key
  tax_processor_key STRING, -- Renamed from employee_tax_processor_key
  fee_processor_key STRING, -- Renamed from employee_fee_processor_key
  lienholder_key STRING,
  -- Descriptive Attributes
  plate_transfer BOOLEAN,
  title_only BOOLEAN,
  buyer_not_lessee BOOLEAN,
  down_payment_status STRING,
  needs_temporary_registration_tags BOOLEAN,
  source_name STRING,
  other_source_description STRING,
  creation_date_utc TIMESTAMP,
  completion_date_utc TIMESTAMP,
  state_asof_utc TIMESTAMP, -- For potential SCD2 tracking
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing descriptive attributes of deals.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes (SCD Type 1 for simplicity, update in place)
MERGE INTO gold.finance.dim_deal AS target
USING (
  SELECT DISTINCT
    s.deal_key,
    s.deal_state_key,
    s.deal_type_key,
    s.source_key,
    s.option_type_key,
    s.title_registration_option_key,
    s.processor_key,
    s.tax_processor_key,
    s.fee_processor_key,
    s.lienholder_key,
    s.plate_transfer,
    s.title_only,
    s.buyer_not_lessee,
    s.down_payment_status,
    s.needs_temporary_registration_tags,
    s.source_name,
    s.other_source_description,
    s.creation_date_utc,
    s.completion_date_utc,
    s.state_asof_utc,
    'silver.finance.dim_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM silver.finance.dim_deal s
  WHERE s.deal_key IS NOT NULL AND s.deal_key != '0'

) AS source
ON target.deal_key = source.deal_key

-- Update existing deals (SCD Type 1 logic)
WHEN MATCHED THEN
  UPDATE SET
    target.deal_state_key = source.deal_state_key,
    target.deal_type_key = source.deal_type_key,
    target.source_key = source.source_key,
    target.option_type_key = source.option_type_key,
    target.title_registration_option_key = source.title_registration_option_key,
    target.processor_key = source.processor_key,
    target.tax_processor_key = source.tax_processor_key,
    target.fee_processor_key = source.fee_processor_key,
    target.lienholder_key = source.lienholder_key,
    target.plate_transfer = source.plate_transfer,
    target.title_only = source.title_only,
    target.buyer_not_lessee = source.buyer_not_lessee,
    target.down_payment_status = source.down_payment_status,
    target.needs_temporary_registration_tags = source.needs_temporary_registration_tags,
    target.source_name = source.source_name,
    target.other_source_description = source.other_source_description,
    target.creation_date_utc = source.creation_date_utc,
    target.completion_date_utc = source.completion_date_utc,
    target.state_asof_utc = source.state_asof_utc,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

-- Insert new deals
WHEN NOT MATCHED THEN
  INSERT (
    deal_key,
    deal_state_key,
    deal_type_key,
    source_key,
    option_type_key,
    title_registration_option_key,
    processor_key,
    tax_processor_key,
    fee_processor_key,
    lienholder_key,
    plate_transfer,
    title_only,
    buyer_not_lessee,
    down_payment_status,
    needs_temporary_registration_tags,
    source_name,
    other_source_description,
    creation_date_utc,
    completion_date_utc,
    state_asof_utc,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_key,
    source.deal_state_key,
    source.deal_type_key,
    source.source_key,
    source.option_type_key,
    source.title_registration_option_key,
    source.processor_key,
    source.tax_processor_key,
    source.fee_processor_key,
    source.lienholder_key,
    source.plate_transfer,
    source.title_only,
    source.buyer_not_lessee,
    source.down_payment_status,
    source.needs_temporary_registration_tags,
    source.source_name,
    source.other_source_description,
    source.creation_date_utc,
    source.completion_date_utc,
    source.state_asof_utc,
    source._source_table,
    source._load_timestamp
  );

-- Add 'Unknown' Deal for referential integrity if needed
MERGE INTO gold.finance.dim_deal AS target
USING (
  SELECT
    '0' as deal_key,
    'Unknown' AS deal_state_key, 'Unknown' AS deal_type_key, 'Unknown' AS source_key,
    'Unknown' AS option_type_key, 'Unknown' AS title_registration_option_key,
    'Unknown' AS processor_key, 'Unknown' AS tax_processor_key, 'Unknown' AS fee_processor_key,
    'Unknown' AS lienholder_key, NULL AS plate_transfer, NULL AS title_only, NULL AS buyer_not_lessee,
    'Unknown' AS down_payment_status, NULL AS needs_temporary_registration_tags, 'Unknown' AS source_name, 'Unknown' AS other_source_description,
    NULL AS creation_date_utc, NULL AS completion_date_utc, NULL AS state_asof_utc,
    'static' AS _source_table, CAST('1900-01-01' AS TIMESTAMP) AS _load_timestamp
) AS source
ON target.deal_key = source.deal_key
WHEN NOT MATCHED THEN INSERT *; 