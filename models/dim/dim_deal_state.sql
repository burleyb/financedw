-- models/dim/dim_deal_state.sql
-- Dimension table for deal state

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS finance_gold.finance.dim_deal_state (
  deal_state_key STRING NOT NULL, -- Natural key from source (e.g., deal_state)
  deal_state_description STRING, -- Potentially more descriptive state (e.g., Funded, Cancelled)
  is_active_state BOOLEAN, -- Flag: Is the deal still actively being worked on towards a final outcome?
  is_final_state BOOLEAN, -- Flag: Has the deal reached a terminal state (funded, cancelled, etc.)?
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing distinct deal states and their attributes.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.columnMapping.mode' = 'name' -- Required for IDENTITY columns
);

-- Add constraint if deal_state_source should be unique (requires DBR 11.1+ or Unity Catalog)
-- ALTER TABLE gold.finance.dim_deal_state ADD CONSTRAINT dim_deal_state_source_unique UNIQUE (deal_state_source);

-- 2. Merge incremental changes
MERGE INTO finance_gold.finance.dim_deal_state AS target
USING (
  -- Source query: Select distinct deal states from source
  WITH source_states AS (
      SELECT DISTINCT deal_state FROM silver.deal.big_deal
  )
  SELECT
    COALESCE(s.deal_state, 'unknown') AS deal_state_key,
    -- Derive description based on known states
    CASE COALESCE(s.deal_state, 'unknown')
      WHEN 'waiting_for_title' THEN 'Waiting for Title'
      WHEN 'signed' THEN 'Signed'
      WHEN 'booted' THEN 'Booted' -- Assuming this means rejected/cancelled
      WHEN 'finalized' THEN 'Finalized'
      WHEN 'sent_to_processor' THEN 'Sent to Processor'
      WHEN 'title_received' THEN 'Title Received'
      WHEN 'closing' THEN 'Closing'
      WHEN 'estimate' THEN 'Estimate'
      WHEN 'structuring' THEN 'Structuring'
      WHEN 'soft_close' THEN 'Soft Close'
      WHEN 'closed' THEN 'Closed'
      WHEN 'send_payoff' THEN 'Send Payoff'
      WHEN 'at_dmv' THEN 'At DMV'
      WHEN 'funded' THEN 'Funded'
      WHEN 'signatures' THEN 'Signatures'
      WHEN 'appointment' THEN 'Appointment'
      ELSE COALESCE(s.deal_state, 'Unknown')
    END AS deal_state_description,
    -- Derive active state flag based on business logic
    CASE
      WHEN s.deal_state IN ('booted', 'finalized', 'closed') THEN false
      WHEN s.deal_state IS NULL THEN false -- NULL is not active
      ELSE true -- All other states are considered active/in progress
    END AS is_active_state,
    -- Derive final state flag based on business logic
    CASE
      WHEN s.deal_state IN ('booted', 'finalized', 'closed', 'funded') THEN true
      ELSE false 
    END AS is_final_state,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_states s
) AS source
ON target.deal_state_key = source.deal_state_key

-- Update existing states if description or flags change
WHEN MATCHED AND (
    target.deal_state_description <> source.deal_state_description OR
    target.is_active_state <> source.is_active_state OR
    target.is_final_state <> source.is_final_state
  ) THEN
  UPDATE SET
    target.deal_state_description = source.deal_state_description,
    target.is_active_state = source.is_active_state,
    target.is_final_state = source.is_final_state,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new states (deal_state_key is auto-generated)
WHEN NOT MATCHED THEN
  INSERT (
    deal_state_key,
    deal_state_description,
    is_active_state,
    is_final_state,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_state_key,
    source.deal_state_description,
    source.is_active_state,
    source.is_final_state,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 