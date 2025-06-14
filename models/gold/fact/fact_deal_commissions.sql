-- models/gold/fact/fact_deal_commissions.sql
-- Gold layer deal commissions fact table with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_commissions (
  deal_id STRING NOT NULL,
  commission_id STRING NOT NULL,
  employee_key STRING,
  driver_key STRING,
  vehicle_key STRING,
  bank_key STRING,
  option_type_key STRING,
  deal_state_key STRING,
  geography_key STRING,
  commission_date_key INT,
  commission_time_key INT,
  
  -- Credit Memo Flags
  has_credit_memo BOOLEAN,
  credit_memo_date_key INT, -- FK to dim_date
  credit_memo_time_key INT, -- FK to dim_time
  
  -- Commission measures (in cents)
  setter_commission_cents BIGINT,
  setter_commission_dollars DECIMAL(15,2),
  closer_commission_cents BIGINT,
  closer_commission_dollars DECIMAL(15,2),
  total_commission_cents BIGINT,
  total_commission_dollars DECIMAL(15,2),
  
  -- Commission metrics
  commission_type STRING,
  commission_rate DECIMAL(5,4),
  deal_value_cents BIGINT,
  deal_value_dollars DECIMAL(15,2),
  commission_percentage DECIMAL(5,2),
  
  -- Business classifications
  commission_tier STRING,
  performance_category STRING,
  is_split_commission BOOLEAN,
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer deal commissions fact table with comprehensive commission analytics'
PARTITIONED BY (commission_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.fact_deal_commissions AS target
USING (
  SELECT
    sfc.deal_id,
    sfc.commission_id,
    sfc.employee_key,
    sfc.driver_key,
    sfc.vehicle_key,
    sfc.bank_key,
    sfc.option_type_key,
    sfc.deal_state_key,
    sfc.geography_key,
    sfc.commission_date_key,
    sfc.commission_time_key,
    
    -- Commission measures
    sfc.setter_commission_cents,
    ROUND(sfc.setter_commission_cents / 100.0, 2) AS setter_commission_dollars,
    sfc.closer_commission_cents,
    ROUND(sfc.closer_commission_cents / 100.0, 2) AS closer_commission_dollars,
    sfc.total_commission_cents,
    ROUND(sfc.total_commission_cents / 100.0, 2) AS total_commission_dollars,
    
    -- Commission metrics
    sfc.commission_type,
    sfc.commission_rate,
    sfc.deal_value_cents,
    ROUND(sfc.deal_value_cents / 100.0, 2) AS deal_value_dollars,
    CASE 
      WHEN sfc.deal_value_cents > 0 THEN 
        ROUND((sfc.total_commission_cents / sfc.deal_value_cents) * 100.0, 2)
      ELSE 0
    END AS commission_percentage,
    
    -- Business classifications
    CASE
      WHEN sfc.total_commission_cents >= 200000 THEN 'High ($2K+)'  -- $2K+ in cents
      WHEN sfc.total_commission_cents >= 100000 THEN 'Medium ($1K-$2K)'
      WHEN sfc.total_commission_cents >= 50000 THEN 'Standard ($500-$1K)'
      WHEN sfc.total_commission_cents > 0 THEN 'Low (<$500)'
      ELSE 'None'
    END AS commission_tier,
    
    CASE
      WHEN sfc.commission_rate >= 0.05 THEN 'High Performer (5%+)'
      WHEN sfc.commission_rate >= 0.03 THEN 'Good Performer (3-5%)'
      WHEN sfc.commission_rate >= 0.01 THEN 'Standard Performer (1-3%)'
      WHEN sfc.commission_rate > 0 THEN 'Low Performer (<1%)'
      ELSE 'No Commission'
    END AS performance_category,
    
    CASE 
      WHEN sfc.setter_commission_cents > 0 AND sfc.closer_commission_cents > 0 THEN TRUE
      ELSE FALSE
    END AS is_split_commission,
    
    -- Metadata
    'silver.finance.fact_deal_commissions' as _source_table,
    
    -- Credit memo flags
    sfc.has_credit_memo,
    sfc.credit_memo_date_key,
    sfc.credit_memo_time_key,
    
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.fact_deal_commissions sfc
) AS source
ON target.deal_id = source.deal_id AND target.commission_id = source.commission_id

WHEN MATCHED THEN
  UPDATE SET
    target.employee_key = source.employee_key,
    target.driver_key = source.driver_key,
    target.vehicle_key = source.vehicle_key,
    target.bank_key = source.bank_key,
    target.option_type_key = source.option_type_key,
    target.deal_state_key = source.deal_state_key,
    target.geography_key = source.geography_key,
    target.commission_date_key = source.commission_date_key,
    target.commission_time_key = source.commission_time_key,
    target.setter_commission_cents = source.setter_commission_cents,
    target.setter_commission_dollars = source.setter_commission_dollars,
    target.closer_commission_cents = source.closer_commission_cents,
    target.closer_commission_dollars = source.closer_commission_dollars,
    target.total_commission_cents = source.total_commission_cents,
    target.total_commission_dollars = source.total_commission_dollars,
    target.commission_type = source.commission_type,
    target.commission_rate = source.commission_rate,
    target.deal_value_cents = source.deal_value_cents,
    target.deal_value_dollars = source.deal_value_dollars,
    target.commission_percentage = source.commission_percentage,
    target.commission_tier = source.commission_tier,
    target.performance_category = source.performance_category,
    target.is_split_commission = source.is_split_commission,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp,
    target.has_credit_memo = source.has_credit_memo,
    target.credit_memo_date_key = source.credit_memo_date_key,
    target.credit_memo_time_key = source.credit_memo_time_key

WHEN NOT MATCHED THEN
  INSERT *
  VALUES (
    source.deal_id,
    source.commission_id,
    source.employee_key,
    source.driver_key,
    source.vehicle_key,
    source.bank_key,
    source.option_type_key,
    source.deal_state_key,
    source.geography_key,
    source.commission_date_key,
    source.commission_time_key,
    source.setter_commission_cents,
    source.setter_commission_dollars,
    source.closer_commission_cents,
    source.closer_commission_dollars,
    source.total_commission_cents,
    source.total_commission_dollars,
    source.commission_type,
    source.commission_rate,
    source.deal_value_cents,
    source.deal_value_dollars,
    source.commission_percentage,
    source.commission_tier,
    source.performance_category,
    source.is_split_commission,
    source._source_table,
    source._load_timestamp,
    source.has_credit_memo,
    source.credit_memo_date_key,
    source.credit_memo_time_key
  ); 