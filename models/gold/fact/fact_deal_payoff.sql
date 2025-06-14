-- models/gold/fact/fact_deal_payoff.sql
-- Gold layer deal payoff fact table with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_payoff (
  deal_id STRING NOT NULL,
  payoff_id STRING,
  driver_key STRING,
  vehicle_key STRING,
  bank_key STRING,
  lienholder_key STRING,
  geography_key STRING,
  payoff_date_key INT,
  payoff_time_key INT,
  
  -- Credit Memo Flags
  has_credit_memo BOOLEAN,
  credit_memo_date_key INT, -- FK to dim_date
  credit_memo_time_key INT, -- FK to dim_time
  
  -- Financial measures (in cents)
  payoff_amount_cents BIGINT,
  payoff_amount_dollars DECIMAL(15,2),
  per_diem_amount_cents BIGINT,
  per_diem_amount_dollars DECIMAL(15,2),
  total_payoff_cents BIGINT,
  total_payoff_dollars DECIMAL(15,2),
  
  -- Payoff metrics
  days_to_payoff INT,
  payoff_method STRING,
  is_expedited_payoff BOOLEAN,
  payoff_status STRING,
  
  -- Business classifications
  payoff_size_category STRING,
  payoff_urgency STRING,
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer deal payoff fact table with comprehensive payoff metrics'
PARTITIONED BY (payoff_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.fact_deal_payoff AS target
USING (
  SELECT
    sfp.deal_id,
    sfp.payoff_id,
    sfp.driver_key,
    sfp.vehicle_key,
    sfp.bank_key,
    sfp.lienholder_key,
    sfp.geography_key,
    sfp.payoff_date_key,
    sfp.payoff_time_key,
    
    -- Financial measures
    sfp.payoff_amount_cents,
    ROUND(sfp.payoff_amount_cents / 100.0, 2) AS payoff_amount_dollars,
    sfp.per_diem_amount_cents,
    ROUND(sfp.per_diem_amount_cents / 100.0, 2) AS per_diem_amount_dollars,
    sfp.total_payoff_cents,
    ROUND(sfp.total_payoff_cents / 100.0, 2) AS total_payoff_dollars,
    
    -- Payoff metrics
    sfp.days_to_payoff,
    sfp.payoff_method,
    sfp.is_expedited_payoff,
    sfp.payoff_status,
    
    -- Business classifications
    CASE
      WHEN sfp.total_payoff_cents >= 5000000 THEN 'Large ($50K+)'  -- $50K+ in cents
      WHEN sfp.total_payoff_cents >= 2500000 THEN 'Medium ($25K-$50K)'
      WHEN sfp.total_payoff_cents >= 1000000 THEN 'Small ($10K-$25K)'
      WHEN sfp.total_payoff_cents > 0 THEN 'Micro (<$10K)'
      ELSE 'Unknown'
    END AS payoff_size_category,
    
    CASE
      WHEN sfp.days_to_payoff <= 1 THEN 'Same Day'
      WHEN sfp.days_to_payoff <= 3 THEN 'Urgent (2-3 days)'
      WHEN sfp.days_to_payoff <= 7 THEN 'Standard (4-7 days)'
      WHEN sfp.days_to_payoff <= 14 THEN 'Extended (8-14 days)'
      WHEN sfp.days_to_payoff > 14 THEN 'Delayed (15+ days)'
      ELSE 'Unknown'
    END AS payoff_urgency,
    
    -- Metadata
    'silver.finance.fact_deal_payoff' as _source_table,
    
    -- Credit memo flags
    sfp.has_credit_memo,
    sfp.credit_memo_date_key,
    sfp.credit_memo_time_key,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.fact_deal_payoff sfp
) AS source
ON target.deal_id = source.deal_id AND target.payoff_id = source.payoff_id

WHEN MATCHED THEN
  UPDATE SET
    target.driver_key = source.driver_key,
    target.vehicle_key = source.vehicle_key,
    target.bank_key = source.bank_key,
    target.lienholder_key = source.lienholder_key,
    target.geography_key = source.geography_key,
    target.payoff_date_key = source.payoff_date_key,
    target.payoff_time_key = source.payoff_time_key,
    target.payoff_amount_cents = source.payoff_amount_cents,
    target.payoff_amount_dollars = source.payoff_amount_dollars,
    target.per_diem_amount_cents = source.per_diem_amount_cents,
    target.per_diem_amount_dollars = source.per_diem_amount_dollars,
    target.total_payoff_cents = source.total_payoff_cents,
    target.total_payoff_dollars = source.total_payoff_dollars,
    target.days_to_payoff = source.days_to_payoff,
    target.payoff_method = source.payoff_method,
    target.is_expedited_payoff = source.is_expedited_payoff,
    target.payoff_status = source.payoff_status,
    target.payoff_size_category = source.payoff_size_category,
    target.payoff_urgency = source.payoff_urgency,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp,
    target.has_credit_memo = source.has_credit_memo,
    target.credit_memo_date_key = source.credit_memo_date_key,
    target.credit_memo_time_key = source.credit_memo_time_key

WHEN NOT MATCHED THEN
  INSERT *
  VALUES (
    source.deal_id,
    source.payoff_id,
    source.driver_key,
    source.vehicle_key,
    source.bank_key,
    source.lienholder_key,
    source.geography_key,
    source.payoff_date_key,
    source.payoff_time_key,
    source.payoff_amount_cents,
    source.payoff_amount_dollars,
    source.per_diem_amount_cents,
    source.per_diem_amount_dollars,
    source.total_payoff_cents,
    source.total_payoff_dollars,
    source.days_to_payoff,
    source.payoff_method,
    source.is_expedited_payoff,
    source.payoff_status,
    source.payoff_size_category,
    source.payoff_urgency,
    source._source_table,
    source._load_timestamp,
    source.has_credit_memo,
    source.credit_memo_date_key,
    source.credit_memo_time_key
  ); 