-- models/fact/fact_deal_payoff.sql
-- Fact table for deal payoffs

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS finance_gold.finance.fact_deal_payoff (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal
  lienholder_key STRING, -- FK to dim_lienholder
  payoff_date_key INT, -- FK to dim_date (likely good_through_date or completion_date)
  payoff_time_key INT, -- FK to dim_time
  payoff_good_through_date_key INT, -- FK to dim_date

  -- Measures (Stored as BIGINT cents)
  vehicle_payoff_amount BIGINT,
  estimated_payoff_amount BIGINT,
  user_entered_total_payoff_amount BIGINT,
  vehicle_cost_amount BIGINT,
  -- Add other relevant payoff measures if needed

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Fact table storing payoff amounts related to deals.'
PARTITIONED BY (payoff_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO finance_gold.finance.fact_deal_payoff AS target
USING (
  SELECT
    d.id AS deal_key,
    COALESCE(d.lienholder_slug, d.lienholder_name, 'Unknown') AS lienholder_key,
    -- Using completion date as the primary date key, adjust if another date is more relevant
    CAST(DATE_FORMAT(COALESCE(d.completion_date_utc, d.creation_date_utc), 'yyyyMMdd') AS INT) AS payoff_date_key,
    CAST(DATE_FORMAT(COALESCE(d.completion_date_utc, d.creation_date_utc), 'HHmmss') AS INT) AS payoff_time_key,
    CAST(DATE_FORMAT(d.good_through_date, 'yyyyMMdd') AS INT) AS payoff_good_through_date_key,

    -- Measures (multiply by 100 and cast to BIGINT)
    CAST(d.vehicle_payoff * 100 AS BIGINT) AS vehicle_payoff_amount,
    CAST(d.estimated_payoff * 100 AS BIGINT) AS estimated_payoff_amount,
    CAST(d.user_entered_total_payoff * 100 AS BIGINT) AS user_entered_total_payoff_amount,
    CAST(d.vehicle_cost * 100 AS BIGINT) AS vehicle_cost_amount,

    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM silver.deal.big_deal d
  -- Filter for relevant deals (e.g., those with payoff info or recent updates)
  WHERE (d.vehicle_payoff IS NOT NULL OR d.estimated_payoff IS NOT NULL OR d.user_entered_total_payoff IS NOT NULL OR d.vehicle_cost IS NOT NULL)
  -- Optional: Add time-based filter for incremental loads
  -- AND d.state_asof_utc > (SELECT MAX(_load_timestamp) FROM gold.finance.fact_deal_payoff WHERE _load_timestamp IS NOT NULL)

  -- Ensure only the latest version of each deal is processed if source has duplicates per batch
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY d.state_asof_utc DESC) = 1

) AS source
ON target.deal_key = source.deal_key
   -- Add date key to match for potential updates on the same deal/day
   AND target.payoff_date_key = source.payoff_date_key

-- Update existing payoff records if amounts or keys change
WHEN MATCHED AND (
    target.lienholder_key <> source.lienholder_key OR
    target.payoff_good_through_date_key <> source.payoff_good_through_date_key OR
    target.vehicle_payoff_amount <> source.vehicle_payoff_amount OR
    target.estimated_payoff_amount <> source.estimated_payoff_amount OR
    target.user_entered_total_payoff_amount <> source.user_entered_total_payoff_amount OR
    target.vehicle_cost_amount <> source.vehicle_cost_amount
  ) THEN
  UPDATE SET
    target.lienholder_key = source.lienholder_key,
    target.payoff_time_key = source.payoff_time_key,
    target.payoff_good_through_date_key = source.payoff_good_through_date_key,
    target.vehicle_payoff_amount = source.vehicle_payoff_amount,
    target.estimated_payoff_amount = source.estimated_payoff_amount,
    target.user_entered_total_payoff_amount = source.user_entered_total_payoff_amount,
    target.vehicle_cost_amount = source.vehicle_cost_amount,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

-- Insert new payoff records
WHEN NOT MATCHED THEN
  INSERT (
    deal_key,
    lienholder_key,
    payoff_date_key,
    payoff_time_key,
    payoff_good_through_date_key,
    vehicle_payoff_amount,
    estimated_payoff_amount,
    user_entered_total_payoff_amount,
    vehicle_cost_amount,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_key,
    source.lienholder_key,
    source.payoff_date_key,
    source.payoff_time_key,
    source.payoff_good_through_date_key,
    source.vehicle_payoff_amount,
    source.estimated_payoff_amount,
    source.user_entered_total_payoff_amount,
    source.vehicle_cost_amount,
    source._source_table,
    source._load_timestamp
  ); 