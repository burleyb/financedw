-- models/gold/fact/fact_deal_milestones.sql
-- Gold layer fact table for deal milestones with business enhancements
DROP TABLE IF EXISTS gold.finance.fact_deal_milestones;

CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_milestones (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal
  milestone_date_key INT, -- FK to dim_date
  milestone_time_key INT, -- FK to dim_time
  deal_state_key STRING, -- FK to dim_deal_state
  user_key STRING, -- FK to dim_user (who triggered the state change)
  
  -- Milestone Classification
  milestone_category STRING, -- initiation, processing, completion, cancellation
  milestone_order INT, -- Workflow sequence order
  state_sequence INT, -- Sequence within this deal's progression
  
  -- Time-based Measures
  days_since_creation INT, -- Days since deal creation
  days_since_previous_milestone INT, -- Days since previous milestone
  hours_since_previous_milestone INT, -- Hours since previous milestone
  
  -- Milestone Flags (derived from deal state dimension)
  is_revenue_milestone BOOLEAN, -- Milestone triggers revenue recognition
  is_completion_milestone BOOLEAN, -- Milestone indicates deal completion
  is_cancellation_milestone BOOLEAN, -- Milestone indicates deal cancellation
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer fact table for deal milestone tracking and workflow analysis'
PARTITIONED BY (milestone_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.fact_deal_milestones AS target
USING (
  SELECT
    sfm.deal_key,
    sfm.milestone_date_key,
    sfm.milestone_time_key,
    sfm.deal_state_key,
    sfm.user_key,
    sfm.milestone_category,
    sfm.milestone_order,
    sfm.state_sequence,
    sfm.days_since_creation,
    sfm.days_since_previous_milestone,
    sfm.hours_since_previous_milestone,
    sfm.is_revenue_milestone,
    sfm.is_completion_milestone,
    sfm.is_cancellation_milestone,
    sfm._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.fact_deal_milestones sfm
  WHERE sfm._load_timestamp > COALESCE((SELECT MAX(_load_timestamp) FROM gold.finance.fact_deal_milestones), '1900-01-01')
) AS source
ON target.deal_key = source.deal_key 
   AND target.milestone_date_key = source.milestone_date_key 
   AND target.deal_state_key = source.deal_state_key

WHEN MATCHED AND (
    target.milestone_time_key != source.milestone_time_key OR
    target.user_key != source.user_key OR
    target.milestone_category != source.milestone_category OR
    target.milestone_order != source.milestone_order OR
    target.state_sequence != source.state_sequence OR
    COALESCE(target.days_since_creation, -1) != COALESCE(source.days_since_creation, -1) OR
    COALESCE(target.days_since_previous_milestone, -1) != COALESCE(source.days_since_previous_milestone, -1) OR
    COALESCE(target.hours_since_previous_milestone, -1) != COALESCE(source.hours_since_previous_milestone, -1) OR
    target.is_revenue_milestone != source.is_revenue_milestone OR
    target.is_completion_milestone != source.is_completion_milestone OR
    target.is_cancellation_milestone != source.is_cancellation_milestone
) THEN
  UPDATE SET
    target.milestone_time_key = source.milestone_time_key,
    target.user_key = source.user_key,
    target.milestone_category = source.milestone_category,
    target.milestone_order = source.milestone_order,
    target.state_sequence = source.state_sequence,
    target.days_since_creation = source.days_since_creation,
    target.days_since_previous_milestone = source.days_since_previous_milestone,
    target.hours_since_previous_milestone = source.hours_since_previous_milestone,
    target.is_revenue_milestone = source.is_revenue_milestone,
    target.is_completion_milestone = source.is_completion_milestone,
    target.is_cancellation_milestone = source.is_cancellation_milestone,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    deal_key,
    milestone_date_key,
    milestone_time_key,
    deal_state_key,
    user_key,
    milestone_category,
    milestone_order,
    state_sequence,
    days_since_creation,
    days_since_previous_milestone,
    hours_since_previous_milestone,
    is_revenue_milestone,
    is_completion_milestone,
    is_cancellation_milestone,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_key,
    source.milestone_date_key,
    source.milestone_time_key,
    source.deal_state_key,
    source.user_key,
    source.milestone_category,
    source.milestone_order,
    source.state_sequence,
    source.days_since_creation,
    source.days_since_previous_milestone,
    source.hours_since_previous_milestone,
    source.is_revenue_milestone,
    source.is_completion_milestone,
    source.is_cancellation_milestone,
    source._source_table,
    source._load_timestamp
  ); 