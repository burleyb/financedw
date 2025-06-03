-- models/gold_big_deal/dim/dim_deal_state.sql
-- Dimension table for deal state (simplified version for big_deal schema)

-- Drop and recreate to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_deal_state_big_deal;

CREATE TABLE gold.finance.dim_deal_state_big_deal (
  deal_state_key STRING NOT NULL, -- Natural key from source (e.g., deal_state)
  deal_state_description STRING, -- Potentially more descriptive state (e.g., Funded, Cancelled)
  is_active_state BOOLEAN, -- Flag: Is the deal still actively being worked on towards a final outcome?
  is_final_state BOOLEAN, -- Flag: Has the deal reached a terminal state (funded, cancelled, etc.)?
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing distinct deal states and their attributes (big_deal version).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert data from big_deal source
INSERT INTO gold.finance.dim_deal_state_big_deal
WITH source_states AS (
    SELECT DISTINCT deal_state FROM silver.deal.big_deal
    WHERE deal_state IS NOT NULL
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
    WHEN 'cancelled' THEN 'Cancelled'
    WHEN 'declined' THEN 'Declined'
    WHEN 'approved' THEN 'Approved'
    WHEN 'pending' THEN 'Pending'
    ELSE COALESCE(s.deal_state, 'Unknown')
  END AS deal_state_description,
  -- Derive active state flag based on business logic
  CASE
    WHEN s.deal_state IN ('booted', 'finalized', 'cancelled', 'declined') THEN false
    WHEN s.deal_state IS NULL THEN false -- NULL is not active
    ELSE true -- All other states are considered active/in progress
  END AS is_active_state,
  -- Derive final state flag based on business logic
  CASE
    WHEN s.deal_state IN ('booted', 'finalized', 'cancelled', 'declined', 'funded') THEN true
    ELSE false 
  END AS is_final_state,
  'silver.deal.big_deal' as _source_table,
  CURRENT_TIMESTAMP() as _load_timestamp
FROM source_states s

UNION ALL

-- Add standard unknown record
SELECT
  'unknown' AS deal_state_key,
  'Unknown' AS deal_state_description,
  false AS is_active_state,
  false AS is_final_state,
  'system' as _source_table,
  CURRENT_TIMESTAMP() as _load_timestamp; 