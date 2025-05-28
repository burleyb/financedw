-- models/gold/fact/fact_deals.sql

-- Gold layer fact table for deals reading from silver layer

CREATE TABLE IF NOT EXISTS gold.finance.fact_deals (
  -- Primary Key
  deal_key STRING NOT NULL,
  
  -- Foreign Keys to Dimensions
  driver_key STRING,
  vehicle_key STRING,
  bank_key STRING,
  employee_key STRING,
  pod_key INT,
  titling_pod_key INT,
  processor_key STRING,
  deal_state_key STRING,
  source_key STRING,
  option_type_key STRING,
  vsc_type_key STRING,
  geography_key STRING,
  employment_key INT,
  
  -- Date/Time Keys
  creation_date_key INT,
  creation_time_key INT,
  completion_date_key INT,
  completion_time_key INT,
  
  -- Financial Measures (stored as BIGINT cents)
  amount_financed_amount BIGINT,
  payment_amount BIGINT,
  money_down_amount BIGINT,
  sell_rate DECIMAL(8,4),
  buy_rate DECIMAL(8,4),
  profit_amount BIGINT,
  
  -- VSC Measures
  vsc_price_amount BIGINT,
  vsc_cost_amount BIGINT,
  vsc_rev_amount BIGINT,
  
  -- GAP Measures
  gap_price_amount BIGINT,
  gap_cost_amount BIGINT,
  gap_rev_amount BIGINT,
  
  -- Fee Measures
  total_fee_amount BIGINT,
  ally_fees_amount BIGINT,
  reserve_amount BIGINT,
  
  -- Commission Measures
  setter_commission_amount BIGINT,
  closer_commission_amount BIGINT,
  
  -- Additional Measures
  term_months SMALLINT,
  days_to_payment INT,
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer fact table for deal transactions with all financial measures'
PARTITIONED BY (creation_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge data from silver layer
MERGE INTO gold.finance.fact_deals AS target
USING (
  SELECT
    -- Primary Key
    fd.deal_key,
    
    -- Foreign Keys (using COALESCE to handle NULLs with "unknown" defaults)
    COALESCE(fd.driver_key, 'unknown') as driver_key,
    COALESCE(fd.vehicle_key, 'unknown') as vehicle_key,
    COALESCE(fd.bank_key, 'unknown') as bank_key,
    COALESCE(fd.employee_key, 'unknown') as employee_key,
    COALESCE(fd.pod_key, -1) as pod_key,
    COALESCE(fd.titling_pod_key, -1) as titling_pod_key,
    COALESCE(fd.processor_key, 'UNKNOWN') as processor_key,
    COALESCE(fd.deal_state_key, 'unknown') as deal_state_key,
    COALESCE(fd.source_key, 'unknown') as source_key,
    COALESCE(fd.option_type_key, 'unknown') as option_type_key,
    COALESCE(fd.vsc_type_key, 'Unknown') as vsc_type_key,
    COALESCE(fd.geography_key, 'Unknown|Unknown|Unknown|Unknown') as geography_key,
    COALESCE(fd.employment_key, -1) as employment_key,
    
    -- Date/Time Keys
    fd.creation_date_key,
    fd.creation_time_key,
    fd.completion_date_key,
    fd.completion_time_key,
    
    -- Financial Measures
    fd.amount_financed_amount,
    fd.payment_amount,
    fd.money_down_amount,
    fd.sell_rate,
    fd.buy_rate,
    fd.profit_amount,
    
    -- VSC Measures
    fd.vsc_price_amount,
    fd.vsc_cost_amount,
    fd.vsc_rev_amount,
    
    -- GAP Measures
    fd.gap_price_amount,
    fd.gap_cost_amount,
    fd.gap_rev_amount,
    
    -- Fee Measures
    fd.total_fee_amount,
    fd.ally_fees_amount,
    fd.reserve_amount,
    
    -- Commission Measures
    fd.setter_commission_amount,
    fd.closer_commission_amount,
    
    -- Additional Measures
    fd.term_months,
    fd.days_to_payment,
    
    -- Metadata
    'silver.finance.fact_deals' as _source_table
    
  FROM silver.finance.fact_deals fd
  WHERE fd.deal_key IS NOT NULL
    AND fd.deal_key != '0'
) AS source
ON target.deal_key = source.deal_key

-- Update existing deals if any measures change
WHEN MATCHED AND (
    target.amount_financed_amount <> source.amount_financed_amount OR
    target.payment_amount <> source.payment_amount OR
    target.profit_amount <> source.profit_amount OR
    target.vsc_rev_amount <> source.vsc_rev_amount OR
    target.gap_rev_amount <> source.gap_rev_amount OR
    target.reserve_amount <> source.reserve_amount OR
    target.deal_state_key <> source.deal_state_key OR
    target.completion_date_key <> source.completion_date_key
  ) THEN
  UPDATE SET
    target.driver_key = source.driver_key,
    target.vehicle_key = source.vehicle_key,
    target.bank_key = source.bank_key,
    target.employee_key = source.employee_key,
    target.pod_key = source.pod_key,
    target.titling_pod_key = source.titling_pod_key,
    target.processor_key = source.processor_key,
    target.deal_state_key = source.deal_state_key,
    target.source_key = source.source_key,
    target.option_type_key = source.option_type_key,
    target.vsc_type_key = source.vsc_type_key,
    target.geography_key = source.geography_key,
    target.employment_key = source.employment_key,
    target.creation_date_key = source.creation_date_key,
    target.creation_time_key = source.creation_time_key,
    target.completion_date_key = source.completion_date_key,
    target.completion_time_key = source.completion_time_key,
    target.amount_financed_amount = source.amount_financed_amount,
    target.payment_amount = source.payment_amount,
    target.money_down_amount = source.money_down_amount,
    target.sell_rate = source.sell_rate,
    target.buy_rate = source.buy_rate,
    target.profit_amount = source.profit_amount,
    target.vsc_price_amount = source.vsc_price_amount,
    target.vsc_cost_amount = source.vsc_cost_amount,
    target.vsc_rev_amount = source.vsc_rev_amount,
    target.gap_price_amount = source.gap_price_amount,
    target.gap_cost_amount = source.gap_cost_amount,
    target.gap_rev_amount = source.gap_rev_amount,
    target.total_fee_amount = source.total_fee_amount,
    target.ally_fees_amount = source.ally_fees_amount,
    target.reserve_amount = source.reserve_amount,
    target.setter_commission_amount = source.setter_commission_amount,
    target.closer_commission_amount = source.closer_commission_amount,
    target.term_months = source.term_months,
    target.days_to_payment = source.days_to_payment,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new deals
WHEN NOT MATCHED THEN
  INSERT (
    deal_key, driver_key, vehicle_key, bank_key, employee_key, pod_key, titling_pod_key,
    processor_key, deal_state_key, source_key, option_type_key, vsc_type_key, geography_key,
    employment_key, creation_date_key, creation_time_key, completion_date_key, completion_time_key,
    amount_financed_amount, payment_amount, money_down_amount, sell_rate, buy_rate, profit_amount,
    vsc_price_amount, vsc_cost_amount, vsc_rev_amount, gap_price_amount, gap_cost_amount, gap_rev_amount,
    total_fee_amount, ally_fees_amount, reserve_amount, setter_commission_amount, closer_commission_amount,
    term_months, days_to_payment, _source_table, _load_timestamp
  )
  VALUES (
    source.deal_key, source.driver_key, source.vehicle_key, source.bank_key, source.employee_key,
    source.pod_key, source.titling_pod_key, source.processor_key, source.deal_state_key, source.source_key,
    source.option_type_key, source.vsc_type_key, source.geography_key, source.employment_key,
    source.creation_date_key, source.creation_time_key, source.completion_date_key, source.completion_time_key,
    source.amount_financed_amount, source.payment_amount, source.money_down_amount, source.sell_rate,
    source.buy_rate, source.profit_amount, source.vsc_price_amount, source.vsc_cost_amount, source.vsc_rev_amount,
    source.gap_price_amount, source.gap_cost_amount, source.gap_rev_amount, source.total_fee_amount,
    source.ally_fees_amount, source.reserve_amount, source.setter_commission_amount, source.closer_commission_amount,
    source.term_months, source.days_to_payment, source._source_table, CURRENT_TIMESTAMP()
  ); 