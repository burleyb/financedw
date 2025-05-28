-- models/silver/dim/dim_deal_state.sql
-- Silver layer deal state dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_deal_state (
  deal_state_key STRING NOT NULL, -- Natural key from source (e.g., deal_state)
  deal_state_description STRING, -- Descriptive state name
  is_active_state BOOLEAN, -- Flag: Is the deal still actively being worked on?
  is_final_state BOOLEAN, -- Flag: Has the deal reached a terminal state?
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Silver layer deal state dimension with business logic flags'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_deal_state AS target
USING (
  -- Source query: Select distinct deal states from bronze deals table
  WITH source_states AS (
      SELECT DISTINCT deal_state 
      FROM bronze.leaseend_db_public.deals
      WHERE _fivetran_deleted = FALSE
        AND deal_state IS NOT NULL
  )
  SELECT
    COALESCE(s.deal_state, 'unknown') AS deal_state_key,
    -- Derive description based on known states
    CASE COALESCE(s.deal_state, 'unknown')
      WHEN 'waiting_for_title' THEN 'Waiting for Title'
      WHEN 'signed' THEN 'Signed'
      WHEN 'booted' THEN 'Booted'
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
    'bronze.leaseend_db_public.deals' as _source_table,
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
    CURRENT_TIMESTAMP() as _load_timestamp
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

-- Insert new states
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