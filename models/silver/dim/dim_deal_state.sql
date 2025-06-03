-- models/silver/dim/dim_deal_state.sql
-- Silver layer deal state dimension table with enhanced business context

-- Drop the existing table to ensure clean recreation with correct schema
DROP TABLE IF EXISTS silver.finance.dim_deal_state;

CREATE TABLE silver.finance.dim_deal_state (
  deal_state_key STRING NOT NULL, -- Natural key from source (e.g., deal_state)
  deal_state_name STRING, -- Friendly display name
  deal_state_description STRING, -- Detailed description
  
  -- Business Categories
  business_category STRING, -- Lead Generation, Deal Processing, Deal Completion, Post-Funding, Cancelled/Rejected
  workflow_stage STRING, -- Initial Contact, Underwriting, Documentation, Funding, Title Transfer, Terminated
  
  -- State Characteristics
  is_active_state BOOLEAN, -- Deal is still being worked on
  is_final_state BOOLEAN, -- Terminal state (no further progression)
  is_revenue_state BOOLEAN, -- State triggers revenue recognition
  is_completion_state BOOLEAN, -- State indicates deal completion
  is_cancellation_state BOOLEAN, -- State indicates deal cancellation
  
  -- Workflow Information
  workflow_order INT, -- Standard progression order
  expected_duration_days INT, -- Typical time spent in this state
  expected_next_states STRING, -- Comma-separated list of typical next states
  
  -- Milestone Flags
  is_initiation_milestone BOOLEAN, -- Deal initiation milestone
  is_processing_milestone BOOLEAN, -- Deal processing milestone
  is_completion_milestone BOOLEAN, -- Deal completion milestone
  is_cancellation_milestone BOOLEAN, -- Deal cancellation milestone
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer deal state dimension with comprehensive business context'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert initial data into the newly created table
INSERT INTO silver.finance.dim_deal_state
WITH source_states AS (
    SELECT DISTINCT state as deal_state 
    FROM bronze.leaseend_db_public.deal_states
    WHERE _fivetran_deleted = FALSE
      AND state IS NOT NULL
    
    UNION
    
    SELECT DISTINCT state as deal_state 
    FROM bronze.leaseend_db_public.deals
    WHERE _fivetran_deleted = FALSE
      AND state IS NOT NULL
)
SELECT
  COALESCE(s.deal_state, 'unknown') AS deal_state_key,
  
  -- Friendly display names
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
  END AS deal_state_name,
  
  -- Detailed descriptions
  CASE COALESCE(s.deal_state, 'unknown')
    WHEN 'waiting_for_title' THEN 'Deal is waiting for vehicle title documentation'
    WHEN 'signed' THEN 'Customer has signed the deal documents'
    WHEN 'booted' THEN 'Deal was removed from the system'
    WHEN 'finalized' THEN 'Deal has been completed and finalized'
    WHEN 'sent_to_processor' THEN 'Deal sent to financial processor for approval'
    WHEN 'title_received' THEN 'Vehicle title has been received'
    WHEN 'closing' THEN 'Deal is in the closing process'
    WHEN 'estimate' THEN 'Initial estimate provided to customer'
    WHEN 'structuring' THEN 'Deal terms are being structured'
    WHEN 'soft_close' THEN 'Deal is in soft close status'
    WHEN 'closed' THEN 'Deal has been closed'
    WHEN 'send_payoff' THEN 'Payoff information is being sent'
    WHEN 'at_dmv' THEN 'Deal is being processed at DMV'
    WHEN 'funded' THEN 'Deal has been funded by the bank'
    WHEN 'signatures' THEN 'Waiting for customer signatures'
    WHEN 'appointment' THEN 'Customer appointment scheduled'
    WHEN 'cancelled' THEN 'Deal was cancelled'
    WHEN 'declined' THEN 'Deal was declined'
    WHEN 'approved' THEN 'Deal has been approved'
    WHEN 'pending' THEN 'Deal is pending review'
    ELSE 'Unknown deal state'
  END AS deal_state_description,
  
  -- Business categories
  CASE COALESCE(s.deal_state, 'unknown')
    WHEN 'estimate' THEN 'Lead Generation'
    WHEN 'appointment' THEN 'Lead Generation'
    WHEN 'pending' THEN 'Deal Processing'
    WHEN 'structuring' THEN 'Deal Processing'
    WHEN 'sent_to_processor' THEN 'Deal Processing'
    WHEN 'approved' THEN 'Deal Processing'
    WHEN 'signatures' THEN 'Deal Processing'
    WHEN 'signed' THEN 'Deal Processing'
    WHEN 'closing' THEN 'Deal Completion'
    WHEN 'soft_close' THEN 'Deal Completion'
    WHEN 'closed' THEN 'Deal Completion'
    WHEN 'funded' THEN 'Deal Completion'
    WHEN 'finalized' THEN 'Deal Completion'
    WHEN 'waiting_for_title' THEN 'Post-Funding'
    WHEN 'title_received' THEN 'Post-Funding'
    WHEN 'send_payoff' THEN 'Post-Funding'
    WHEN 'at_dmv' THEN 'Post-Funding'
    WHEN 'booted' THEN 'Cancelled/Rejected'
    WHEN 'cancelled' THEN 'Cancelled/Rejected'
    WHEN 'declined' THEN 'Cancelled/Rejected'
    ELSE 'Unknown'
  END AS business_category,
  
  -- Workflow stages
  CASE COALESCE(s.deal_state, 'unknown')
    WHEN 'estimate' THEN 'Initial Contact'
    WHEN 'appointment' THEN 'Initial Contact'
    WHEN 'pending' THEN 'Underwriting'
    WHEN 'structuring' THEN 'Underwriting'
    WHEN 'sent_to_processor' THEN 'Underwriting'
    WHEN 'approved' THEN 'Documentation'
    WHEN 'signatures' THEN 'Documentation'
    WHEN 'signed' THEN 'Documentation'
    WHEN 'closing' THEN 'Funding'
    WHEN 'soft_close' THEN 'Funding'
    WHEN 'closed' THEN 'Funding'
    WHEN 'funded' THEN 'Funding'
    WHEN 'finalized' THEN 'Funding'
    WHEN 'waiting_for_title' THEN 'Title Transfer'
    WHEN 'title_received' THEN 'Title Transfer'
    WHEN 'send_payoff' THEN 'Title Transfer'
    WHEN 'at_dmv' THEN 'Title Transfer'
    WHEN 'booted' THEN 'Terminated'
    WHEN 'cancelled' THEN 'Terminated'
    WHEN 'declined' THEN 'Terminated'
    ELSE 'Unknown'
  END AS workflow_stage,
  
  -- State characteristics
  CASE
    WHEN s.deal_state IN ('booted', 'finalized', 'cancelled', 'declined', 'closed', 'funded') THEN false
    WHEN s.deal_state IS NULL THEN false
    ELSE true
  END AS is_active_state,
  
  CASE
    WHEN s.deal_state IN ('booted', 'finalized', 'cancelled', 'declined', 'funded', 'closed') THEN true
    ELSE false 
  END AS is_final_state,
  
  CASE WHEN s.deal_state = 'signed' THEN true ELSE false END AS is_revenue_state,
  
  CASE WHEN s.deal_state IN ('finalized', 'closed', 'funded') THEN true ELSE false END AS is_completion_state,
  
  CASE WHEN s.deal_state IN ('booted', 'cancelled', 'declined') THEN true ELSE false END AS is_cancellation_state,
  
  -- Workflow order
  CASE s.deal_state
    WHEN 'estimate' THEN 1
    WHEN 'appointment' THEN 2
    WHEN 'pending' THEN 3
    WHEN 'structuring' THEN 4
    WHEN 'sent_to_processor' THEN 5
    WHEN 'approved' THEN 6
    WHEN 'signatures' THEN 7
    WHEN 'signed' THEN 8
    WHEN 'closing' THEN 9
    WHEN 'soft_close' THEN 10
    WHEN 'closed' THEN 11
    WHEN 'funded' THEN 12
    WHEN 'finalized' THEN 13
    WHEN 'waiting_for_title' THEN 14
    WHEN 'title_received' THEN 15
    WHEN 'send_payoff' THEN 16
    WHEN 'at_dmv' THEN 17
    WHEN 'booted' THEN 99
    WHEN 'cancelled' THEN 98
    WHEN 'declined' THEN 97
    ELSE 0
  END AS workflow_order,
  
  -- Expected duration in days
  CASE s.deal_state
    WHEN 'estimate' THEN 1
    WHEN 'appointment' THEN 1
    WHEN 'pending' THEN 2
    WHEN 'structuring' THEN 3
    WHEN 'sent_to_processor' THEN 5
    WHEN 'approved' THEN 1
    WHEN 'signatures' THEN 2
    WHEN 'signed' THEN 1
    WHEN 'closing' THEN 3
    WHEN 'soft_close' THEN 2
    WHEN 'closed' THEN 1
    WHEN 'funded' THEN 1
    WHEN 'finalized' THEN 1
    WHEN 'waiting_for_title' THEN 14
    WHEN 'title_received' THEN 7
    WHEN 'send_payoff' THEN 3
    WHEN 'at_dmv' THEN 10
    ELSE NULL
  END AS expected_duration_days,
  
  -- Expected next states
  CASE s.deal_state
    WHEN 'estimate' THEN 'appointment,cancelled'
    WHEN 'appointment' THEN 'pending,cancelled'
    WHEN 'pending' THEN 'structuring,declined'
    WHEN 'structuring' THEN 'sent_to_processor,cancelled'
    WHEN 'sent_to_processor' THEN 'approved,declined'
    WHEN 'approved' THEN 'signatures,cancelled'
    WHEN 'signatures' THEN 'signed,cancelled'
    WHEN 'signed' THEN 'closing,cancelled'
    WHEN 'closing' THEN 'soft_close,closed,cancelled'
    WHEN 'soft_close' THEN 'closed,cancelled'
    WHEN 'closed' THEN 'funded'
    WHEN 'funded' THEN 'finalized,waiting_for_title'
    WHEN 'finalized' THEN 'waiting_for_title'
    WHEN 'waiting_for_title' THEN 'title_received'
    WHEN 'title_received' THEN 'send_payoff,at_dmv'
    WHEN 'send_payoff' THEN 'at_dmv'
    WHEN 'at_dmv' THEN NULL
    ELSE NULL
  END AS expected_next_states,
  
  -- Milestone flags
  CASE WHEN s.deal_state IN ('estimate', 'appointment') THEN true ELSE false END AS is_initiation_milestone,
  CASE WHEN s.deal_state IN ('structuring', 'sent_to_processor', 'signatures', 'signed') THEN true ELSE false END AS is_processing_milestone,
  CASE WHEN s.deal_state IN ('closing', 'soft_close', 'closed', 'funded', 'finalized') THEN true ELSE false END AS is_completion_milestone,
  CASE WHEN s.deal_state IN ('booted', 'cancelled', 'declined') THEN true ELSE false END AS is_cancellation_milestone,
  
  'bronze.leaseend_db_public.deal_states' as _source_table,
  CURRENT_TIMESTAMP() as _load_timestamp
FROM source_states s

UNION ALL

-- Add standard unknown record
SELECT
  'unknown' AS deal_state_key,
  'Unknown' AS deal_state_name,
  'Unknown deal state' AS deal_state_description,
  'Unknown' AS business_category,
  'Unknown' AS workflow_stage,
  false AS is_active_state,
  false AS is_final_state,
  false AS is_revenue_state,
  false AS is_completion_state,
  false AS is_cancellation_state,
  0 AS workflow_order,
  NULL AS expected_duration_days,
  NULL AS expected_next_states,
  false AS is_initiation_milestone,
  false AS is_processing_milestone,
  false AS is_completion_milestone,
  false AS is_cancellation_milestone,
  'system' as _source_table,
  CURRENT_TIMESTAMP() as _load_timestamp; 