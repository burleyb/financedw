-- models/fact/fact_payoffs.sql
-- Fact table for payoff transactions

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.fact_payoffs (
  transaction_id_source STRING, -- Degenerate dimension
  event_date_key INT,
  good_through_date_key INT,
  deal_id_source INT, -- Degenerate dimension link to deals
  lienholder_id_source STRING, -- Consider creating dim_lienholder
  payoff_amount DECIMAL(10, 2),
  payoff_status STRING, -- Consider making a dimension
  _created_at TIMESTAMP,
  _updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Fact table containing payoff transaction details.';

-- 2. Implement MERGE logic for incremental updates
MERGE INTO gold.fact_payoffs AS target
USING (
  -- Source query: Select payoff data and join dimensions
  SELECT
    te.transaction_id AS transaction_id_source,
    COALESCE(dd_event.date_key, -1) AS event_date_key,
    COALESCE(dd_good_thru.date_key, -1) AS good_through_date_key,
    te.deal_id AS deal_id_source,
    te.lienholder_id AS lienholder_id_source, -- Assuming this column exists
    CAST(te.amount AS DECIMAL(10, 2)) AS payoff_amount,
    te.status AS payoff_status,
    te.event_date -- Assuming event_date tracks updates
  FROM silver.deal.transaction_event te
  LEFT JOIN gold.dim_date dd_event ON DATE(te.event_date) = dd_event.date_actual
  LEFT JOIN gold.dim_date dd_good_thru ON DATE(te.good_through_date) = dd_good_thru.date_actual
  WHERE te.transaction_type = 'Payoff' -- Filter for payoff events
  -- Optional: Add incremental filtering based on event_date or another timestamp
) AS source
ON target.transaction_id_source = source.transaction_id_source

-- Update existing payoffs
WHEN MATCHED THEN
  UPDATE SET
    target.event_date_key = source.event_date_key,
    target.good_through_date_key = source.good_through_date_key,
    target.deal_id_source = source.deal_id_source,
    target.lienholder_id_source = source.lienholder_id_source,
    target.payoff_amount = source.payoff_amount,
    target.payoff_status = source.payoff_status,
    target._updated_at = CURRENT_TIMESTAMP()

-- Insert new payoffs
WHEN NOT MATCHED THEN
  INSERT (
    transaction_id_source, event_date_key, good_through_date_key, deal_id_source,
    lienholder_id_source, payoff_amount, payoff_status,
    _created_at, _updated_at
  )
  VALUES (
    source.transaction_id_source, source.event_date_key, source.good_through_date_key, source.deal_id_source,
    source.lienholder_id_source, source.payoff_amount, source.payoff_status,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );