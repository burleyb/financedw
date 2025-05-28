-- models/silver/dim/dim_deal.sql
-- Dimension table for core deal attributes sourcing directly from bronze tables

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS silver.finance.dim_deal (
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
MERGE INTO silver.finance.dim_deal AS target
USING (
  WITH latest_financial_infos AS (
    SELECT 
      deal_id,
      option_type,
      processor,
      tax_processor,
      fee_processor,
      plate_transfer,
      title_only,
      buyer_not_lessee,
      down_payment_status,
      needs_temporary_registration_tags,
      title_registration_option,
      ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY updated_at DESC) as rn
    FROM bronze.leaseend_db_public.financial_infos
    WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
  ),
  
  latest_deal_states AS (
    SELECT 
      deal_id,
      state,
      CAST(updated_date_utc AS TIMESTAMP) as state_asof_utc,
      ROW_NUMBER() OVER (PARTITION BY deal_id, state ORDER BY updated_date_utc DESC) as rn
    FROM bronze.leaseend_db_public.deal_states
    WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
  ),
  
  latest_cars AS (
    SELECT 
      deal_id,
      id as car_id,
      ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY updated_at DESC) as rn
    FROM bronze.leaseend_db_public.cars
    WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
  ),
  
  latest_payoffs AS (
    SELECT 
      car_id,
      lienholder_name,
      lienholder_slug,
      ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY updated_at DESC) as rn
    FROM bronze.leaseend_db_public.payoffs
    WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
  ),
  
  latest_referrals AS (
    SELECT 
      deal_id,
      source_name,
      other_source_description,
      ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY updated_at DESC) as rn
    FROM bronze.leaseend_db_public.deals_referrals_sources
    WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
  )
  
  SELECT DISTINCT
    d.id AS deal_key,
    COALESCE(d.state, 'Unknown') AS deal_state_key,
    COALESCE(d.type, 'Unknown') AS deal_type_key,
    COALESCE(d.source, 'Unknown') AS source_key,
    COALESCE(fi.option_type, 'noProducts') AS option_type_key,
    COALESCE(fi.title_registration_option, 'Unknown') AS title_registration_option_key,
    COALESCE(UPPER(fi.processor), 'UNKNOWN') AS processor_key,
    COALESCE(UPPER(fi.tax_processor), 'UNKNOWN') AS tax_processor_key,
    COALESCE(UPPER(fi.fee_processor), 'UNKNOWN') AS fee_processor_key,
    COALESCE(p.lienholder_slug, p.lienholder_name, 'Unknown') AS lienholder_key,
    COALESCE(CAST(fi.plate_transfer AS BOOLEAN), FALSE) AS plate_transfer,
    COALESCE(CAST(fi.title_only AS BOOLEAN), FALSE) AS title_only,
    COALESCE(CAST(fi.buyer_not_lessee AS BOOLEAN), FALSE) AS buyer_not_lessee,
    COALESCE(fi.down_payment_status, 'Unknown') AS down_payment_status,
    COALESCE(CAST(fi.needs_temporary_registration_tags AS BOOLEAN), FALSE) AS needs_temporary_registration_tags,
    COALESCE(r.source_name, 'Unknown') AS source_name,
    COALESCE(r.other_source_description, 'Unknown') AS other_source_description,
    d.creation_date_utc,
    d.completion_date_utc,
    COALESCE(ds.state_asof_utc, d.updated_at) as state_asof_utc,
    'bronze.leaseend_db_public.deals+financial_infos+payoffs+referrals' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM bronze.leaseend_db_public.deals d
  LEFT JOIN latest_financial_infos fi ON d.id = fi.deal_id AND fi.rn = 1
  LEFT JOIN latest_deal_states ds ON d.id = ds.deal_id AND d.state = ds.state AND ds.rn = 1
  LEFT JOIN latest_cars c ON d.id = c.deal_id AND c.rn = 1
  LEFT JOIN latest_payoffs p ON c.car_id = p.car_id AND p.rn = 1
  LEFT JOIN latest_referrals r ON d.id = r.deal_id AND r.rn = 1
  WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
    AND d.state IS NOT NULL 
    AND d.id != 0

  -- Ensure only the latest version of each deal is processed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY COALESCE(ds.state_asof_utc, d.updated_at) DESC) = 1
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
MERGE INTO silver.finance.dim_deal AS target
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