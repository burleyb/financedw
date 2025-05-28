-- models/silver/fact/fact_deal_netsuite.sql
-- Silver layer fact table for NetSuite related deal aggregates
-- NOTE: This is a simplified version that can be enhanced with full NetSuite integration

CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_netsuite (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal
  netsuite_posting_date_key INT, -- FK to dim_date
  netsuite_posting_time_key INT, -- FK to dim_time

  -- Basic NetSuite Measures (Stored as BIGINT cents)
  -- These would be populated from bronze.ns tables when available
  gross_profit BIGINT,
  gross_margin BIGINT, -- Percentage * 10000 (e.g., 12.34% stored as 1234)
  
  -- Placeholder for future NetSuite account-based measures
  -- These can be added when NetSuite integration is fully implemented
  total_revenue BIGINT,
  total_costs BIGINT,

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer fact table for NetSuite financial aggregates (simplified version)'
PARTITIONED BY (netsuite_posting_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- For now, create a placeholder merge that can be enhanced later
-- This creates the table structure but doesn't populate it until NetSuite integration is complete
MERGE INTO silver.finance.fact_deal_netsuite AS target
USING (
  -- Placeholder query - this would be replaced with actual NetSuite data
  SELECT 
    'placeholder' as deal_key,
    0 as netsuite_posting_date_key,
    0 as netsuite_posting_time_key,
    CAST(0 AS BIGINT) as gross_profit,
    CAST(0 AS BIGINT) as gross_margin,
    CAST(0 AS BIGINT) as total_revenue,
    CAST(0 AS BIGINT) as total_costs,
    'placeholder' as _source_table
  WHERE 1=0 -- This ensures no records are actually inserted
) AS source
ON target.deal_key = source.deal_key

WHEN NOT MATCHED THEN
  INSERT (
    deal_key,
    netsuite_posting_date_key,
    netsuite_posting_time_key,
    gross_profit,
    gross_margin,
    total_revenue,
    total_costs,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_key,
    source.netsuite_posting_date_key,
    source.netsuite_posting_time_key,
    source.gross_profit,
    source.gross_margin,
    source.total_revenue,
    source.total_costs,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- TODO: Enhance this table with full NetSuite integration
-- This would involve:
-- 1. Reading from bronze.ns.* tables
-- 2. Implementing the complex account-based aggregations from bigdealnscreation.py
-- 3. Adding all the specific NetSuite revenue and cost accounts
-- 4. Proper VIN-based matching with deals 