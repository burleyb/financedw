-- models/silver/fact/fact_deals.sql

-- Primary deals fact table sourced from bronze tables

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
  rpt_amount BIGINT, -- Revenue/Profit Type?
  ally_fees_amount BIGINT,

  -- Other Measures
  term INT,
  days_to_payment INT,

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (creation_date_key) -- Partitioning by date key is common for fact tables
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true', 
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO silver.finance.fact_deals AS target
USING (
  -- Select the latest deal data and join with dimensions to get foreign keys
  WITH deal_data AS (
    SELECT
      d.id,
      d.state as deal_state,
      d.type as deal_type,
      d.customer_id,
      d.creation_date_utc,
      d.completion_date_utc,
      d.source,
      d.updated_at,
      ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY d.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.deals d
    WHERE d.id IS NOT NULL
  ),
  car_data AS (
    SELECT
      c.deal_id,
      LOWER(c.vin) as vin,
      c.updated_at,
      ROW_NUMBER() OVER (PARTITION BY c.deal_id ORDER BY c.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.cars c
    WHERE c.deal_id IS NOT NULL AND c.vin IS NOT NULL
  ),
  financial_data AS (
    SELECT
      fi.deal_id,
      fi.amount_financed,
      fi.payment,
      fi.money_down,
      fi.sell_rate,
      fi.buy_rate,
      fi.profit,
      fi.vsc_price,
      fi.vsc_cost,
      fi.gap_price,
      fi.gap_cost,
      fi.total_fee_amount,
      fi.doc_fee,
      fi.bank_fees,
      fi.registration_transfer_fee,
      fi.title,
      fi.new_registration_fee,
      fi.reserve,
      fi.base_tax_amount,
      fi.warranty_tax_amount,
      fi.option_type,
      fi.bank,
      fi.term,
      fi.days_to_payment,
      -- Calculate derived fields
      CASE 
        WHEN fi.option_type = 'vsc' THEN 675
        WHEN fi.option_type = 'gap' THEN 50
        WHEN fi.option_type = 'vscPlusGap' THEN 725
        ELSE 0
      END as ally_fees,
      CASE 
        WHEN fi.option_type IN ('vsc', 'vscPlusGap') 
        THEN COALESCE(fi.vsc_price, 0) - COALESCE(fi.vsc_cost, 0)
        ELSE 0
      END as vsc_rev,
      CASE 
        WHEN fi.option_type IN ('gap', 'vscPlusGap') 
        THEN COALESCE(fi.gap_price, 0) - COALESCE(fi.gap_cost, 0)
        ELSE 0
      END as gap_rev,
      fi.updated_at,
      ROW_NUMBER() OVER (PARTITION BY fi.deal_id ORDER BY fi.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.financial_infos fi
    WHERE fi.deal_id IS NOT NULL
  )
  SELECT DISTINCT
    CAST(dd.id AS STRING) AS deal_key,
    COALESCE(CAST(dd.deal_state AS STRING), 'Unknown') AS deal_state_key,
    COALESCE(CAST(dd.deal_type AS STRING), 'Unknown') AS deal_type_key,
    COALESCE(CAST(dd.customer_id AS STRING), 'Unknown') AS driver_key,
    COALESCE(CAST(cd.vin AS STRING), 'Unknown') AS vehicle_key,
    COALESCE(CAST(fd.bank AS STRING), 'No Bank') AS bank_key,
    COALESCE(CAST(fd.option_type AS STRING), 'noProducts') as option_type_key,
    COALESCE(CAST(DATE_FORMAT(dd.creation_date_utc, 'yyyyMMdd') AS INT), 0) AS creation_date_key,
    COALESCE(CAST(DATE_FORMAT(dd.creation_date_utc, 'HHmmss') AS INT), 0) AS creation_time_key,
    COALESCE(CAST(DATE_FORMAT(dd.completion_date_utc, 'yyyyMMdd') AS INT), 0) AS completion_date_key,
    COALESCE(CAST(DATE_FORMAT(dd.completion_date_utc, 'HHmmss') AS INT), 0) AS completion_time_key,

    -- Measures (multiply currency by 100, rates by 100, cast to BIGINT, use COALESCE to default nulls to zero)
    CAST(COALESCE(fd.amount_financed, 0) * 100 AS BIGINT) as amount_financed_amount,
    CAST(COALESCE(fd.payment, 0) * 100 AS BIGINT) as payment_amount,
    CAST(COALESCE(fd.money_down, 0) * 100 AS BIGINT) as money_down_amount,
    CAST(COALESCE(fd.sell_rate, 0) * 100 AS BIGINT) as sell_rate_amount,
    CAST(COALESCE(fd.buy_rate, 0) * 100 AS BIGINT) as buy_rate_amount,
    CAST(COALESCE(fd.profit, 0) * 100 AS BIGINT) as profit_amount,
    CAST(COALESCE(fd.vsc_price, 0) * 100 AS BIGINT) as vsc_price_amount,
    CAST(COALESCE(fd.vsc_cost, 0) * 100 AS BIGINT) as vsc_cost_amount,
    CAST(COALESCE(fd.vsc_rev, 0) * 100 AS BIGINT) as vsc_rev_amount,
    CAST(COALESCE(fd.gap_price, 0) * 100 AS BIGINT) as gap_price_amount,
    CAST(COALESCE(fd.gap_cost, 0) * 100 AS BIGINT) as gap_cost_amount,
    CAST(COALESCE(fd.gap_rev, 0) * 100 AS BIGINT) as gap_rev_amount,
    CAST(COALESCE(fd.total_fee_amount, 0) * 100 AS BIGINT) as total_fee_amount,
    CAST(COALESCE(fd.doc_fee, 0) * 100 AS BIGINT) as doc_fee_amount,
    CAST(COALESCE(fd.bank_fees, 0) * 100 AS BIGINT) as bank_fees_amount,
    CAST(COALESCE(fd.registration_transfer_fee, 0) * 100 AS BIGINT) as registration_transfer_fee_amount,
    CAST(COALESCE(fd.title, 0) * 100 AS BIGINT) as title_fee_amount,
    CAST(COALESCE(fd.new_registration_fee, 0) * 100 AS BIGINT) as new_registration_fee_amount,
    CAST(COALESCE(fd.reserve, 0) * 100 AS BIGINT) as reserve_amount,
    CAST(COALESCE(fd.base_tax_amount, 0) * 100 AS BIGINT) as base_tax_amount,
    CAST(COALESCE(fd.warranty_tax_amount, 0) * 100 AS BIGINT) as warranty_tax_amount,
    CAST(COALESCE(fd.ally_fees, 0) * 100 AS BIGINT) as ally_fees_amount,
    CAST(COALESCE(
      COALESCE(fd.title, 0) + 
      COALESCE(fd.doc_fee, 0) + 
      COALESCE(fd.profit, 0) + 
      COALESCE(fd.ally_fees, 0), 0
    ) * 100 AS BIGINT) as rpt_amount,

    CAST(fd.term AS INT) as term,
    CAST(fd.days_to_payment AS INT) as days_to_payment,

    'bronze.leaseend_db_public.deals' as _source_table

  FROM deal_data dd
  LEFT JOIN car_data cd ON dd.id = cd.deal_id AND cd.rn = 1
  LEFT JOIN financial_data fd ON dd.id = fd.deal_id AND fd.rn = 1
  WHERE dd.rn = 1
    AND dd.deal_state IS NOT NULL 
    AND dd.id != 0

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
    target.ally_fees_amount = source.ally_fees_amount,
    target.term = source.term,
    target.days_to_payment = source.days_to_payment,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

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
    ally_fees_amount,
    term,
    days_to_payment,
    _source_table,
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
    source.ally_fees_amount,
    source.term,
    source.days_to_payment,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 