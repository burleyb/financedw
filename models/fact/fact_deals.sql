-- models/fact/fact_deals.sql

-- Ensures the primary deals fact table exists and incrementally updates it.

-- 1. Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS gold.finance.fact_deals (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal
  deal_state_key STRING,
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
  rpt_amount BIGINT, -- Revenue/Profit Type?

  -- Other Measures
  term INT,
  days_to_payment INT,

  -- Metadata
  _source_file_name STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (creation_date_key) -- Partitioning by date key is common for fact tables
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

-- 2. Merge incremental changes
MERGE INTO gold.finance.fact_deals AS target
USING (
  -- Select the latest deal data and join with dimensions to get foreign keys
  SELECT
    d.id AS deal_key,
    COALESCE(CAST(d.deal_state AS STRING), 'Unknown') AS deal_state_key,
    COALESCE(CAST(d.customer_id AS STRING), 'Unknown') AS driver_key, -- Adjust FK lookup if needed
	  COALESCE(CAST(d.vin AS STRING), 'Unknown') AS vehicle_key, -- Adjust FK lookup if needed
    COALESCE(CAST(d.bank AS STRING), 'Unknown') AS bank_key,
    -- Derive option_type_key based on logic in dim_option_type
    CONCAT(COALESCE(CAST(d.vsc_type AS STRING), 'N/A'), '|', CAST(CASE WHEN d.option_type IN ('vscPlusGap', 'gap') THEN true ELSE false END AS STRING)) as option_type_key,
	  CAST(DATE_FORMAT(d.creation_date_utc, 'yyyyMMdd') AS INT) AS creation_date_key,
	  CAST(DATE_FORMAT(d.creation_date_utc, 'HHmmss') AS INT) AS creation_time_key,
    CAST(DATE_FORMAT(d.completion_date_utc, 'yyyyMMdd') AS INT) AS completion_date_key,
	  CAST(DATE_FORMAT(d.completion_date_utc, 'HHmmss') AS INT) AS completion_time_key,

    -- Measures (multiply currency by 100, rates by 100, cast to BIGINT, use COALESCE to default nulls to zero)
    CAST(COALESCE(d.amount_financed, 0) * 100 AS BIGINT) as amount_financed_amount,
    CAST(COALESCE(d.payment, 0) * 100 AS BIGINT) as payment_amount,
    CAST(COALESCE(d.money_down, 0) * 100 AS BIGINT) as money_down_amount,
    CAST(COALESCE(d.sell_rate, 0) * 100 AS BIGINT) as sell_rate_amount,
    CAST(COALESCE(d.buy_rate, 0) * 100 AS BIGINT) as buy_rate_amount,
    CAST(COALESCE(d.profit, 0) * 100 AS BIGINT) as profit_amount,
    CAST(COALESCE(d.vsc_price, 0) * 100 AS BIGINT) as vsc_price_amount,
    CAST(COALESCE(d.vsc_cost, 0) * 100 AS BIGINT) as vsc_cost_amount,
    CAST(COALESCE(d.vsc_rev, 0) * 100 AS BIGINT) as vsc_rev_amount,
    CAST(COALESCE(d.gap_price, 0) * 100 AS BIGINT) as gap_price_amount,
    CAST(COALESCE(d.gap_cost, 0) * 100 AS BIGINT) as gap_cost_amount,
    CAST(COALESCE(d.gap_rev, 0) * 100 AS BIGINT) as gap_rev_amount,
    CAST(COALESCE(d.total_fee_amount, 0) * 100 AS BIGINT) as total_fee_amount,
    CAST(COALESCE(d.doc_fee, 0) * 100 AS BIGINT) as doc_fee_amount,
    CAST(COALESCE(d.bank_fees, 0) * 100 AS BIGINT) as bank_fees_amount,
    CAST(COALESCE(d.registration_transfer_fee, 0) * 100 AS BIGINT) as registration_transfer_fee_amount,
    CAST(COALESCE(d.title_fee, 0) * 100 AS BIGINT) as title_fee_amount,
    CAST(COALESCE(d.new_registration_fee, 0) * 100 AS BIGINT) as new_registration_fee_amount,
    CAST(COALESCE(d.reserve, 0) * 100 AS BIGINT) as reserve_amount,
    CAST(COALESCE(d.base_tax_amount, 0) * 100 AS BIGINT) as base_tax_amount,
    CAST(COALESCE(d.warranty_tax_amount, 0) * 100 AS BIGINT) as warranty_tax_amount,
    CAST(COALESCE(d.rpt, 0) * 100 AS BIGINT) as rpt_amount,

    CAST(d.term AS INT) as term,
    CAST(d.days_to_payment AS INT) as days_to_payment,

    'silver.deal.big_deal' as _source_file_name,
    CURRENT_TIMESTAMP() as _load_timestamp

  FROM silver.deal.big_deal d
  -- Optional: Filter for recent changes if delta source is available
  -- WHERE d.state_asof_utc > (SELECT MAX(_load_timestamp) FROM gold.finance.fact_deals WHERE _load_timestamp IS NOT NULL)

  -- Ensure only the latest version of each deal is processed if source has duplicates per batch
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY d.state_asof_utc DESC) = 1

) AS source
ON target.deal_key = source.deal_key
  -- No date key needed in ON clause if grain is one row per deal_key

-- Update existing deals if relevant measures or dimension keys have changed
WHEN MATCHED THEN
  UPDATE SET
    target.driver_key = source.driver_key,
    target.deal_state_key = source.deal_state_key,
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
