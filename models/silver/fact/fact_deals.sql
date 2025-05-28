-- models/silver/fact/fact_deals.sql

-- Fact table sourcing directly from bronze tables for silver layer
-- This replicates the logic from bigdealleaseendcreation.py in SQL form

-- 1. Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS silver.finance.fact_deals (
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
MERGE INTO silver.finance.fact_deals AS target
USING (
  WITH latest_financial_infos AS (
    SELECT 
      deal_id,
      money_down,
      title,
      total_fee_amount,
      doc_fee,
      vsc_price,
      vsc_cost,
      gap_price,
      gap_cost,
      days_to_payment,
      first_payment_date,
      sell_rate,
      term,
      bank,
      vsc_term,
      payment,
      amount_financed,
      buy_rate,
      profit,
      option_type,
      setter_commission,
      closer_commission,
      plate_transfer,
      atc_quote_name,
      sales_tax_rate,
      reserve,
      vsc_type,
      title_only,
      processor,
      commissionable_gross_revenue,
      ns_invoice_date,
      base_tax_amount,
      warranty_tax_amount,
      tax_processor,
      fee_processor,
      com_rate_markup,
      approval_on_deal_processing,
      finished_documents_screen,
      buyer_not_lessee,
      reached_documents_screen,
      down_payment_status,
      bank_fees,
      user_entered_reserve,
      card_payment_amount_limit,
      registration_transfer_fee,
      title_fee,
      new_registration_fee,
      title_registration_option,
      pen_maintenance_contract_number,
      credit_card_payment_amount_limit,
      quick_notes,
      needs_temporary_registration_tags,
      commissionable_profit,
      -- Calculate vsc_rev
      CASE 
        WHEN option_type IN ('vsc', 'vscPlusGap') THEN COALESCE(vsc_price, 0) - COALESCE(vsc_cost, 0)
        ELSE 0
      END AS vsc_rev,
      -- Calculate gap_rev
      CASE 
        WHEN option_type IN ('gap', 'vscPlusGap') THEN COALESCE(gap_price, 0) - COALESCE(gap_cost, 0)
        ELSE 0
      END AS gap_rev,
      ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY updated_at DESC) as rn
    FROM bronze.leaseend_db_public.financial_infos
    WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
  ),
  
  latest_cars AS (
    SELECT 
      deal_id,
      LOWER(vin) as vin,
      color,
      mileage,
      book_value,
      CAST(year AS INT) as model_year,
      UPPER(make) as make,
      UPPER(model) as model,
      retail_book_value,
      fuel_type,
      vehicle_type,
      kbb_valuation_date,
      kbb_vehicle_name,
      kbb_lending_mileage_adjustment,
      kbb_lending_option_adjustment,
      kbb_retail_mileage_adjustment,
      kbb_retail_option_adjustment,
      kbb_trim_name,
      mmr,
      license_plate_number,
      license_plate_state,
      registration_expiration,
      odometer_status,
      jdp_adjusted_clean_retail,
      jdp_adjusted_clean_trade,
      jdp_mileage_adjustment,
      jdp_trim_body,
      jdp_valuation_date,
      jdp_vehicle_accessories,
      ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY updated_at DESC) as rn
    FROM bronze.leaseend_db_public.cars
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
  
  deal_data AS (
    SELECT DISTINCT
      d.id AS deal_key,
      COALESCE(CAST(d.state AS STRING), 'Unknown') AS deal_state_key,
      COALESCE(CAST(d.type AS STRING), 'Unknown') AS deal_type_key,
      COALESCE(CAST(d.customer_id AS STRING), 'Unknown') AS driver_key,
      COALESCE(CAST(c.vin AS STRING), 'Unknown') AS vehicle_key,
      COALESCE(CAST(fi.bank AS STRING), 'No Bank') AS bank_key,
      COALESCE(CAST(fi.option_type AS STRING), 'noProducts') as option_type_key,
      COALESCE(CAST(DATE_FORMAT(d.creation_date_utc, 'yyyyMMdd') AS INT), 0) AS creation_date_key,
      COALESCE(CAST(DATE_FORMAT(d.creation_date_utc, 'HHmmss') AS INT), 0) AS creation_time_key,
      COALESCE(CAST(DATE_FORMAT(d.completion_date_utc, 'yyyyMMdd') AS INT), 0) AS completion_date_key,
      COALESCE(CAST(DATE_FORMAT(d.completion_date_utc, 'HHmmss') AS INT), 0) AS completion_time_key,

      -- Measures (multiply currency by 100, rates by 100, cast to BIGINT, use COALESCE to default nulls to zero)
      CAST(COALESCE(fi.amount_financed, 0) * 100 AS BIGINT) as amount_financed_amount,
      CAST(COALESCE(fi.payment, 0) * 100 AS BIGINT) as payment_amount,
      CAST(COALESCE(fi.money_down, 0) * 100 AS BIGINT) as money_down_amount,
      CAST(COALESCE(fi.sell_rate, 0) * 100 AS BIGINT) as sell_rate_amount,
      CAST(COALESCE(fi.buy_rate, 0) * 100 AS BIGINT) as buy_rate_amount,
      CAST(COALESCE(fi.profit, 0) * 100 AS BIGINT) as profit_amount,
      CAST(COALESCE(fi.vsc_price, 0) * 100 AS BIGINT) as vsc_price_amount,
      CAST(COALESCE(fi.vsc_cost, 0) * 100 AS BIGINT) as vsc_cost_amount,
      CAST(COALESCE(fi.vsc_rev, 0) * 100 AS BIGINT) as vsc_rev_amount,
      CAST(COALESCE(fi.gap_price, 0) * 100 AS BIGINT) as gap_price_amount,
      CAST(COALESCE(fi.gap_cost, 0) * 100 AS BIGINT) as gap_cost_amount,
      CAST(COALESCE(fi.gap_rev, 0) * 100 AS BIGINT) as gap_rev_amount,
      CAST(COALESCE(fi.total_fee_amount, 0) * 100 AS BIGINT) as total_fee_amount,
      CAST(COALESCE(fi.doc_fee, 0) * 100 AS BIGINT) as doc_fee_amount,
      CAST(COALESCE(fi.bank_fees, 0) * 100 AS BIGINT) as bank_fees_amount,
      CAST(COALESCE(fi.registration_transfer_fee, 0) * 100 AS BIGINT) as registration_transfer_fee_amount,
      CAST(COALESCE(fi.title_fee, 0) * 100 AS BIGINT) as title_fee_amount,
      CAST(COALESCE(fi.new_registration_fee, 0) * 100 AS BIGINT) as new_registration_fee_amount,
      CAST(COALESCE(fi.reserve, 0) * 100 AS BIGINT) as reserve_amount,
      CAST(COALESCE(fi.base_tax_amount, 0) * 100 AS BIGINT) as base_tax_amount,
      CAST(COALESCE(fi.warranty_tax_amount, 0) * 100 AS BIGINT) as warranty_tax_amount,
      -- Calculate rpt as title + doc_fee + profit (matching existing schema)
      CAST(COALESCE(
        COALESCE(fi.title, 0) + 
        COALESCE(fi.doc_fee, 0) + 
        COALESCE(fi.profit, 0), 0) * 100 AS BIGINT) as rpt_amount,

      CAST(fi.term AS INT) as term,
      CAST(fi.days_to_payment AS INT) as days_to_payment,

      'bronze.leaseend_db_public.deals+financial_infos+cars' as _source_file_name,
      CURRENT_TIMESTAMP() as _load_timestamp,
      
      -- Include state_asof_utc for deduplication
      COALESCE(ds.state_asof_utc, d.updated_at) as state_asof_utc

    FROM bronze.leaseend_db_public.deals d
    LEFT JOIN latest_financial_infos fi ON d.id = fi.deal_id AND fi.rn = 1
    LEFT JOIN latest_cars c ON d.id = c.deal_id AND c.rn = 1
    LEFT JOIN latest_deal_states ds ON d.id = ds.deal_id AND d.state = ds.state AND ds.rn = 1
    WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
      AND d.state IS NOT NULL 
      AND d.id != 0

    -- Ensure only the latest version of each deal is processed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY COALESCE(ds.state_asof_utc, d.updated_at) DESC) = 1
  )
  
  SELECT * FROM deal_data

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