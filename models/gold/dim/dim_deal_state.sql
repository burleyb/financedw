-- models/gold/dim/dim_deal_state.sql
-- Gold layer deal state dimension with business enhancements

-- Drop and recreate to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_deal_state;

CREATE TABLE gold.finance.dim_deal_state (
  deal_state_key STRING NOT NULL,
  deal_state_name STRING,
  deal_state_description STRING,
  
  -- Business Categories
  business_category STRING,
  workflow_stage STRING,
  
  -- State Characteristics
  is_active_state BOOLEAN,
  is_final_state BOOLEAN,
  is_revenue_state BOOLEAN,
  is_completion_state BOOLEAN,
  is_cancellation_state BOOLEAN,
  
  -- Workflow Information
  workflow_order INT,
  expected_duration_days INT,
  expected_next_states STRING,
  
  -- Milestone Flags
  is_initiation_milestone BOOLEAN,
  is_processing_milestone BOOLEAN,
  is_completion_milestone BOOLEAN,
  is_cancellation_milestone BOOLEAN,
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer deal state dimension with comprehensive business workflow classifications'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert data from silver layer (only if silver table exists and has the correct schema)
INSERT INTO gold.finance.dim_deal_state
SELECT
  sds.deal_state_key,
  sds.deal_state_name,
  sds.deal_state_description,
  sds.business_category,
  sds.workflow_stage,
  sds.is_active_state,
  sds.is_final_state,
  sds.is_revenue_state,
  sds.is_completion_state,
  sds.is_cancellation_state,
  sds.workflow_order,
  sds.expected_duration_days,
  sds.expected_next_states,
  sds.is_initiation_milestone,
  sds.is_processing_milestone,
  sds.is_completion_milestone,
  sds.is_cancellation_milestone,
  sds._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_deal_state sds; 