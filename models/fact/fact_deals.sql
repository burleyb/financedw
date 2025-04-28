-- models/fact/fact_deals.sql

-- Ensures the primary deals fact table exists and incrementally updates it.

-- 1. Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS finance_gold.finance.fact_deals (
  -- Keys
  deal_id STRING NOT NULL, -- Natural key from source
  driver_key STRING, -- FK to dim_driver (using BIGINT for IDENTITY)
  vehicle_key STRING, -- FK to dim_vehicle (using BIGINT for IDENTITY)
  employee_key STRING, -- FK to dim_employee
  creation_date_key INT, -- FK to dim_date
  creation_time_key INT, -- FK to dim_time
  completion_date_key INT, -- FK to dim_date
  completion_time_key INT, -- FK to dim_time
  deal_state_key STRING, -- FK to dim_deal_state
  option_type_key STRING, -- FK to dim_option_type
  title_registration_option_key STRING, -- FK to dim_title_registration_option
  deal_type_key STRING, -- FK to dim_deal_type
  source_key STRING, -- FK to dim_source
  amount_financed BIGINT,
  payment BIGINT,

  money_down BIGINT,
  sell_rate BIGINT,
  buy_rate BIGINT,
  profit BIGINT,
  vsc_price BIGINT,
  vsc_cost BIGINT,
  gap_price BIGINT,
  gap_cost BIGINT,
  total_fee_amount BIGINT,
  reserve BIGINT,
  setter_commission BIGINT,
  closer_commission BIGINT,
  term BIGINT,
  days_to_payment BIGINT,

  -- Metadata
  _source_file_name STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (creation_date_key) -- Partitioning by date key is common for fact tables
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');

-- 2. Merge incremental changes
MERGE INTO finance_gold.finance.fact_deals AS target
USING (
  -- Select the latest deal data and join with dimensions to get foreign keys
  SELECT
    d.id AS deal_id,
    COALESCE(d.customer_id, 'Unknown') AS driver_key,
	  COALESCE(d.vin, 'Unknown') AS vehicle_key,
    COALESCE(d.processor, 'Unknown') AS employee_key, 
	  CAST(DATE_FORMAT(d.creation_date_utc, 'yyyyMMdd') AS INT) AS creation_date_key,
	  CAST(DATE_FORMAT(d.creation_date_utc, 'HHmmss') AS INT) AS creation_time_key,
    CAST(DATE_FORMAT(d.completion_date_utc, 'yyyyMMdd') AS INT) AS completion_date_key,
	  CAST(DATE_FORMAT(d.completion_date_utc, 'HHmmss') AS INT) AS completion_time_key,
    COALESCE(d.deal_state, 'Unknown') AS deal_state_key,  
    COALESCE(d.option_type, 'Unknown') AS option_type_key,
    COALESCE(d.type, 'Unknown') AS deal_type_key,
    COALESCE(d.source, 'Unknown') AS source_key,
    COALESCE(d.title_registration_option, 'Unknown') AS title_registration_option_key,

    -- Measures (cast to appropriate types if needed)
    CAST(d.amount_financed * 100 AS BIGINT),
    CAST(d.payment * 100 AS BIGINT),
    CAST(d.money_down * 100 AS BIGINT),
    CAST(d.sell_rate * 100 AS BIGINT),
    CAST(d.buy_rate * 100 AS BIGINT),
    CAST(d.profit * 100 AS BIGINT),
    CAST(d.vsc_price AS BIGINT),
    CAST(d.vsc_cost AS BIGINT),
    CAST(d.gap_price AS BIGINT),
    CAST(d.gap_cost AS BIGINT),
    CAST(d.total_fee_amount AS BIGINT),
    CAST(d.reserve AS BIGINT),
    CAST(d.setter_commission AS BIGINT),
    CAST(d.closer_commission AS BIGINT),
    CAST(d.term AS INT),
    CAST(d.days_to_payment AS INT),
    'silver.deal.big_deal' as _source_file_name,
    CURRENT_TIMESTAMP() as _load_timestamp

  FROM silver.deal.big_deal d
  WHERE 
	  d.id > 0 AND 
    d.state_asof_utc > (SELECT MAX(_load_timestamp) FROM finance_gold.finance.fact_deals WHERE _load_timestamp IS NOT NULL)

  -- Ensure only the latest version of each deal is processed if source has duplicates per batch
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY d.state_asof_utc DESC) = 1

) AS source
ON target.deal_id = source.deal_id

-- Update existing deals if relevant measures or dimension keys have changed
WHEN MATCHED THEN
  UPDATE SET
    target.driver_key = source.driver_key,
    target.vehicle_key = source.vehicle_key,
    target.employee_key = source.employee_key,
    target.creation_date_key = source.creation_date_key,
    target.creation_time_key = source.creation_time_key,
    target.completion_date_key = source.completion_date_key,
    target.completion_time_key = source.completion_time_key,
    target.deal_state_key = source.deal_state_key,
    target.option_type_key = source.option_type_key,
    target.title_registration_option_key = source.title_registration_option_key,
    target.deal_type_key = source.deal_type_key,
    target.source_key = source.source_key,
    target.amount_financed = source.amount_financed,
    target.payment = source.payment,
    target.money_down = source.money_down,
    target.sell_rate = source.sell_rate,
    target.buy_rate = source.buy_rate,
    target.profit = source.profit,
    target.vsc_price = source.vsc_price,
    target.vsc_cost = source.vsc_cost,
    target.gap_price = source.gap_price,
    target.gap_cost = source.gap_cost,
    target.total_fee_amount = source.total_fee_amount,
    target.reserve = source.reserve,
    target.setter_commission = source.setter_commission,
    target.closer_commission = source.closer_commission,
    target.term = source.term,
    target.days_to_payment = source.days_to_payment,
    target._source_file_name = source._source_file_name,
    target._load_timestamp = current_timestamp()

-- Insert new deals
WHEN NOT MATCHED THEN
  INSERT (
    deal_id,
    driver_key,
    vehicle_key,
    employee_key,
    creation_date_key,
    creation_time_key,
    completion_date_key,
    completion_time_key,
    deal_state_key,
    option_type_key,
    title_registration_option_key,
    deal_type_key,
    source_key,
    amount_financed,
    payment,
    money_down,
    sell_rate,
    buy_rate,
    profit,
    vsc_price,
    vsc_cost,
    gap_price,
    gap_cost,
    total_fee_amount,
    reserve,
    setter_commission,
    closer_commission,
    term,
    days_to_payment,
    _source_file_name,
    _load_timestamp
  )
  VALUES (
    source.deal_id,
    source.driver_key,
    source.vehicle_key,
    source.employee_key,
    source.creation_date_key,
    source.creation_time_key,
    source.completion_date_key,
    source.completion_time_key,
    source.deal_state_key,
    source.option_type_key,
    source.title_registration_option_key,
    source.deal_type_key,
    source.source_key,
    source.amount_financed,
    source.payment,
    source.money_down,
    source.sell_rate,
    source.buy_rate,
    source.profit,
    source.vsc_price,
    source.vsc_cost,
    source.gap_price,
    source.gap_cost,
    source.total_fee_amount,
    source.reserve,
    source.setter_commission,
    source.closer_commission,
    source.term,
    source.days_to_payment,
    source._source_file_name,
    source._load_timestamp
  );
