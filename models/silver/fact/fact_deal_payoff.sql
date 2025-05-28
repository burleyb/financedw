-- models/silver/fact/fact_deal_payoff.sql
-- Silver layer fact table for deal payoffs reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_payoff (
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

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer fact table storing payoff amounts related to deals'
PARTITIONED BY (payoff_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze sources
MERGE INTO silver.finance.fact_deal_payoff AS target
USING (
  WITH deal_data AS (
    SELECT
      d.id,
      d.lienholder_slug,
      d.lienholder_name,
      d.creation_date_utc,
      d.completion_date_utc,
      d.good_through_date,
      d.vehicle_payoff,
      d.estimated_payoff,
      d.user_entered_total_payoff,
      d.vehicle_cost,
      d.state as deal_state,
      d.updated_at,
      ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY d.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.deals d
    WHERE d.id IS NOT NULL
      AND d._fivetran_deleted = FALSE
      -- Filter for deals with payoff information
      AND (d.vehicle_payoff IS NOT NULL 
           OR d.estimated_payoff IS NOT NULL 
           OR d.user_entered_total_payoff IS NOT NULL 
           OR d.vehicle_cost IS NOT NULL)
  )
  SELECT DISTINCT
    CAST(dd.id AS STRING) AS deal_key,
    COALESCE(dd.lienholder_slug, dd.lienholder_name, 'Unknown') AS lienholder_key,
    -- Using completion date as the primary date key, fallback to creation date
    COALESCE(
      CAST(DATE_FORMAT(dd.completion_date_utc, 'yyyyMMdd') AS INT),
      CAST(DATE_FORMAT(dd.creation_date_utc, 'yyyyMMdd') AS INT),
      0
    ) AS payoff_date_key,
    COALESCE(
      CAST(DATE_FORMAT(dd.completion_date_utc, 'HHmmss') AS INT),
      CAST(DATE_FORMAT(dd.creation_date_utc, 'HHmmss') AS INT),
      0
    ) AS payoff_time_key,
    COALESCE(
      CAST(DATE_FORMAT(dd.good_through_date, 'yyyyMMdd') AS INT),
      0
    ) AS payoff_good_through_date_key,

    -- Measures (multiply by 100 and cast to BIGINT, use COALESCE to default nulls to zero)
    CAST(COALESCE(dd.vehicle_payoff, 0) * 100 AS BIGINT) AS vehicle_payoff_amount,
    CAST(COALESCE(dd.estimated_payoff, 0) * 100 AS BIGINT) AS estimated_payoff_amount,
    CAST(COALESCE(dd.user_entered_total_payoff, 0) * 100 AS BIGINT) AS user_entered_total_payoff_amount,
    CAST(COALESCE(dd.vehicle_cost, 0) * 100 AS BIGINT) AS vehicle_cost_amount,

    'bronze.leaseend_db_public.deals' as _source_table

  FROM deal_data dd
  WHERE dd.rn = 1
    AND dd.deal_state IS NOT NULL 
    AND dd.id != 0

) AS source
ON target.deal_key = source.deal_key
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
    target._load_timestamp = CURRENT_TIMESTAMP()

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
    CURRENT_TIMESTAMP()
  ); 