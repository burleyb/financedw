-- models/gold/fact/fact_deals.sql

-- Gold layer fact table sourcing from silver layer
-- This table provides business-ready, optimized data for analytics and reporting

-- 1. Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS gold.finance.fact_deals (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal
  deal_state_key STRING,
  deal_type_key STRING,
  driver_key STRING, -- FK to dim_driver
  vehicle_key STRING, -- FK to dim_vehicle
  bank_key STRING, -- FK to dim_bank
  option_type_key STRING, -- FK to dim_option_type
  creation_date_key INT, -- FK to dim_date
  creation_time_key INT, -- FK to dim_time
  completion_date_key INT, -- FK to dim_date
  completion_time_key INT, -- FK to dim_time

  -- Core Deal Measures (Stored as BIGINT cents)
  amount_financed_amount BIGINT,
  payment_amount BIGINT,
  money_down_amount BIGINT,
  sell_rate_amount BIGINT, -- Rate * 100
  buy_rate_amount BIGINT,  -- Rate * 100
  profit_amount BIGINT,
  vsc_price_amount BIGINT,
  vsc_cost_amount BIGINT,
  vsc_rev_amount BIGINT,
  gap_price_amount BIGINT,
  gap_cost_amount BIGINT,
  gap_rev_amount BIGINT,
  total_fee_amount BIGINT,
  doc_fee_amount BIGINT,
  bank_fees_amount BIGINT,
  registration_transfer_fee_amount BIGINT,
  title_fee_amount BIGINT,
  new_registration_fee_amount BIGINT,
  reserve_amount BIGINT,
  base_tax_amount BIGINT,
  warranty_tax_amount BIGINT,
  rpt_amount BIGINT, -- Revenue/Profit Type

  -- Other Measures
  term INT,
  days_to_payment INT,

  -- Metadata
  _source_file_name STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (creation_date_key)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

-- 2. Merge incremental changes
MERGE INTO gold.finance.fact_deals AS target
USING (
  -- Select the latest deal data from silver layer
  SELECT DISTINCT
    s.deal_key,
    s.deal_state_key,
    s.deal_type_key,
    s.driver_key,
    s.vehicle_key,
    s.bank_key,
    s.option_type_key,
    s.creation_date_key,
    s.creation_time_key,
    s.completion_date_key,
    s.completion_time_key,

    -- Pass through all measures from silver (already in correct format)
    s.amount_financed_amount,
    s.payment_amount,
    s.money_down_amount,
    s.sell_rate_amount,
    s.buy_rate_amount,
    s.profit_amount,
    s.vsc_price_amount,
    s.vsc_cost_amount,
    s.vsc_rev_amount,
    s.gap_price_amount,
    s.gap_cost_amount,
    s.gap_rev_amount,
    s.total_fee_amount,
    s.doc_fee_amount,
    s.bank_fees_amount,
    s.registration_transfer_fee_amount,
    s.title_fee_amount,
    s.new_registration_fee_amount,
    s.reserve_amount,
    s.base_tax_amount,
    s.warranty_tax_amount,
    s.rpt_amount,

    s.term,
    s.days_to_payment,

    'silver.finance.fact_deals' as _source_file_name,
    CURRENT_TIMESTAMP() as _load_timestamp

  FROM silver.finance.fact_deals s
  WHERE s.deal_key IS NOT NULL AND s.deal_key != '0'

) AS source
ON target.deal_key = source.deal_key

-- Update existing deals if relevant measures or dimension keys have changed
WHEN MATCHED THEN
  UPDATE SET
    target.driver_key = source.driver_key,
    target.deal_state_key = source.deal_state_key,
    target.deal_type_key = source.deal_type_key,
    target.vehicle_key = source.vehicle_key,
    target.bank_key = source.bank_key,
    target.option_type_key = source.option_type_key,
    target.creation_date_key = source.creation_date_key,
    target.creation_time_key = source.creation_time_key,
    target.completion_date_key = source.completion_date_key,
    target.completion_time_key = source.completion_time_key,
    target.amount_financed_amount = source.amount_financed_amount,
    target.payment_amount = source.payment_amount,
    target.money_down_amount = source.money_down_amount,
    target.sell_rate_amount = source.sell_rate_amount,
    target.buy_rate_amount = source.buy_rate_amount,
    target.profit_amount = source.profit_amount,
    target.vsc_price_amount = source.vsc_price_amount,
    target.vsc_cost_amount = source.vsc_cost_amount,
    target.vsc_rev_amount = source.vsc_rev_amount,
    target.gap_price_amount = source.gap_price_amount,
    target.gap_cost_amount = source.gap_cost_amount,
    target.gap_rev_amount = source.gap_rev_amount,
    target.total_fee_amount = source.total_fee_amount,
    target.doc_fee_amount = source.doc_fee_amount,
    target.bank_fees_amount = source.bank_fees_amount,
    target.registration_transfer_fee_amount = source.registration_transfer_fee_amount,
    target.title_fee_amount = source.title_fee_amount,
    target.new_registration_fee_amount = source.new_registration_fee_amount,
    target.reserve_amount = source.reserve_amount,
    target.base_tax_amount = source.base_tax_amount,
    target.warranty_tax_amount = source.warranty_tax_amount,
    target.rpt_amount = source.rpt_amount,
    target.term = source.term,
    target.days_to_payment = source.days_to_payment,
    target._source_file_name = source._source_file_name,
    target._load_timestamp = current_timestamp()

-- Insert new deals
WHEN NOT MATCHED THEN
  INSERT (
    deal_key,
    deal_state_key,
    deal_type_key,
    driver_key,
    vehicle_key,
    bank_key,
    option_type_key,
    creation_date_key,
    creation_time_key,
    completion_date_key,
    completion_time_key,
    amount_financed_amount,
    payment_amount,
    money_down_amount,
    sell_rate_amount,
    buy_rate_amount,
    profit_amount,
    vsc_price_amount,
    vsc_cost_amount,
    vsc_rev_amount,
    gap_price_amount,
    gap_cost_amount,
    gap_rev_amount,
    total_fee_amount,
    doc_fee_amount,
    bank_fees_amount,
    registration_transfer_fee_amount,
    title_fee_amount,
    new_registration_fee_amount,
    reserve_amount,
    base_tax_amount,
    warranty_tax_amount,
    rpt_amount,
    term,
    days_to_payment,
    _source_file_name,
    _load_timestamp
  )
  VALUES (
    source.deal_key,
    source.deal_state_key,
    source.deal_type_key,
    source.driver_key,
    source.vehicle_key,
    source.bank_key,
    source.option_type_key,
    source.creation_date_key,
    source.creation_time_key,
    source.completion_date_key,
    source.completion_time_key,
    source.amount_financed_amount,
    source.payment_amount,
    source.money_down_amount,
    source.sell_rate_amount,
    source.buy_rate_amount,
    source.profit_amount,
    source.vsc_price_amount,
    source.vsc_cost_amount,
    source.vsc_rev_amount,
    source.gap_price_amount,
    source.gap_cost_amount,
    source.gap_rev_amount,
    source.total_fee_amount,
    source.doc_fee_amount,
    source.bank_fees_amount,
    source.registration_transfer_fee_amount,
    source.title_fee_amount,
    source.new_registration_fee_amount,
    source.reserve_amount,
    source.base_tax_amount,
    source.warranty_tax_amount,
    source.rpt_amount,
    source.term,
    source.days_to_payment,
    source._source_file_name,
    source._load_timestamp
  ); 