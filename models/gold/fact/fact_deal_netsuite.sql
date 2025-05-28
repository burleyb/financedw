-- models/gold/fact/fact_deal_netsuite.sql
-- Gold layer deal NetSuite fact table with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_netsuite (
  deal_id STRING NOT NULL,
  netsuite_transaction_id STRING,
  driver_key STRING,
  vehicle_key STRING,
  bank_key STRING,
  option_type_key STRING,
  deal_state_key STRING,
  geography_key STRING,
  transaction_date_key INT,
  transaction_time_key INT,
  
  -- NetSuite financial measures (in cents)
  transaction_amount_cents BIGINT,
  transaction_amount_dollars DECIMAL(15,2),
  revenue_amount_cents BIGINT,
  revenue_amount_dollars DECIMAL(15,2),
  cost_amount_cents BIGINT,
  cost_amount_dollars DECIMAL(15,2),
  profit_amount_cents BIGINT,
  profit_amount_dollars DECIMAL(15,2),
  
  -- NetSuite transaction details
  transaction_type STRING,
  account_name STRING,
  account_number STRING,
  department STRING,
  class STRING,
  location STRING,
  memo STRING,
  
  -- Business classifications
  transaction_category STRING,
  profit_margin_percentage DECIMAL(5,2),
  revenue_size_category STRING,
  
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer deal NetSuite fact table with comprehensive financial accounting metrics'
PARTITIONED BY (transaction_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.fact_deal_netsuite AS target
USING (
  SELECT
    sfn.deal_id,
    sfn.netsuite_transaction_id,
    sfn.driver_key,
    sfn.vehicle_key,
    sfn.bank_key,
    sfn.option_type_key,
    sfn.deal_state_key,
    sfn.geography_key,
    sfn.transaction_date_key,
    sfn.transaction_time_key,
    
    -- Financial measures
    sfn.transaction_amount_cents,
    ROUND(sfn.transaction_amount_cents / 100.0, 2) AS transaction_amount_dollars,
    sfn.revenue_amount_cents,
    ROUND(sfn.revenue_amount_cents / 100.0, 2) AS revenue_amount_dollars,
    sfn.cost_amount_cents,
    ROUND(sfn.cost_amount_cents / 100.0, 2) AS cost_amount_dollars,
    sfn.profit_amount_cents,
    ROUND(sfn.profit_amount_cents / 100.0, 2) AS profit_amount_dollars,
    
    -- Transaction details
    sfn.transaction_type,
    sfn.account_name,
    sfn.account_number,
    sfn.department,
    sfn.class,
    sfn.location,
    sfn.memo,
    
    -- Business classifications
    CASE
      WHEN UPPER(sfn.transaction_type) LIKE '%REVENUE%' OR UPPER(sfn.transaction_type) LIKE '%INCOME%' THEN 'Revenue'
      WHEN UPPER(sfn.transaction_type) LIKE '%COST%' OR UPPER(sfn.transaction_type) LIKE '%EXPENSE%' THEN 'Cost'
      WHEN UPPER(sfn.transaction_type) LIKE '%COMMISSION%' THEN 'Commission'
      WHEN UPPER(sfn.transaction_type) LIKE '%FEE%' THEN 'Fee'
      ELSE 'Other'
    END AS transaction_category,
    
    CASE 
      WHEN sfn.revenue_amount_cents > 0 THEN 
        ROUND(((sfn.revenue_amount_cents - sfn.cost_amount_cents) / sfn.revenue_amount_cents) * 100.0, 2)
      ELSE 0
    END AS profit_margin_percentage,
    
    CASE
      WHEN sfn.revenue_amount_cents >= 500000 THEN 'Large ($5K+)'  -- $5K+ in cents
      WHEN sfn.revenue_amount_cents >= 250000 THEN 'Medium ($2.5K-$5K)'
      WHEN sfn.revenue_amount_cents >= 100000 THEN 'Small ($1K-$2.5K)'
      WHEN sfn.revenue_amount_cents > 0 THEN 'Micro (<$1K)'
      ELSE 'None'
    END AS revenue_size_category,
    
    sfn._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.fact_deal_netsuite sfn
) AS source
ON target.deal_id = source.deal_id AND target.netsuite_transaction_id = source.netsuite_transaction_id

WHEN MATCHED THEN
  UPDATE SET
    target.driver_key = source.driver_key,
    target.vehicle_key = source.vehicle_key,
    target.bank_key = source.bank_key,
    target.option_type_key = source.option_type_key,
    target.deal_state_key = source.deal_state_key,
    target.geography_key = source.geography_key,
    target.transaction_date_key = source.transaction_date_key,
    target.transaction_time_key = source.transaction_time_key,
    target.transaction_amount_cents = source.transaction_amount_cents,
    target.transaction_amount_dollars = source.transaction_amount_dollars,
    target.revenue_amount_cents = source.revenue_amount_cents,
    target.revenue_amount_dollars = source.revenue_amount_dollars,
    target.cost_amount_cents = source.cost_amount_cents,
    target.cost_amount_dollars = source.cost_amount_dollars,
    target.profit_amount_cents = source.profit_amount_cents,
    target.profit_amount_dollars = source.profit_amount_dollars,
    target.transaction_type = source.transaction_type,
    target.account_name = source.account_name,
    target.account_number = source.account_number,
    target.department = source.department,
    target.class = source.class,
    target.location = source.location,
    target.memo = source.memo,
    target.transaction_category = source.transaction_category,
    target.profit_margin_percentage = source.profit_margin_percentage,
    target.revenue_size_category = source.revenue_size_category,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT *
  VALUES (
    source.deal_id,
    source.netsuite_transaction_id,
    source.driver_key,
    source.vehicle_key,
    source.bank_key,
    source.option_type_key,
    source.deal_state_key,
    source.geography_key,
    source.transaction_date_key,
    source.transaction_time_key,
    source.transaction_amount_cents,
    source.transaction_amount_dollars,
    source.revenue_amount_cents,
    source.revenue_amount_dollars,
    source.cost_amount_cents,
    source.cost_amount_dollars,
    source.profit_amount_cents,
    source.profit_amount_dollars,
    source.transaction_type,
    source.account_name,
    source.account_number,
    source.department,
    source.class,
    source.location,
    source.memo,
    source.transaction_category,
    source.profit_margin_percentage,
    source.revenue_size_category,
    source._source_table,
    source._load_timestamp
  ); 