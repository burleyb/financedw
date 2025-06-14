-- models/silver/fact/fact_deal_milestones.sql
-- Fact table for tracking all deal state transitions and milestones
-- This provides detailed workflow analysis capabilities

DROP TABLE IF EXISTS silver.finance.fact_deal_milestones;

CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_milestones (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal
  milestone_date_key INT, -- FK to dim_date
  milestone_time_key INT, -- FK to dim_time
  deal_state_key STRING, -- FK to dim_deal_state
  user_key STRING, -- FK to dim_user (who triggered the state change)
  
  -- Credit Memo Flags
  has_credit_memo BOOLEAN,
  credit_memo_date_key INT, -- FK to dim_date
  credit_memo_time_key INT, -- FK to dim_time
  
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
COMMENT 'Silver layer fact table for deal milestone tracking and workflow analysis'
PARTITIONED BY (milestone_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze deal_states
MERGE INTO silver.finance.fact_deal_milestones AS target
USING (
  WITH deal_creation_dates AS (
    SELECT 
      CAST(d.id AS STRING) as deal_id,
      d.creation_date_utc
    FROM bronze.leaseend_db_public.deals d
    WHERE d.id IS NOT NULL 
      AND (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
  ),
  
  deal_state_transitions AS (
    SELECT
      CAST(ds.deal_id AS STRING) as deal_key,
      ds.state as deal_state_key,
      ds.updated_date_utc as milestone_date_utc,
      COALESCE(ds.user_id, 'system') as user_key,
      ROW_NUMBER() OVER (PARTITION BY ds.deal_id ORDER BY ds.updated_date_utc ASC) as state_sequence,
      LAG(ds.updated_date_utc) OVER (PARTITION BY ds.deal_id ORDER BY ds.updated_date_utc ASC) as previous_milestone_date,
      ds.updated_at,
      ROW_NUMBER() OVER (PARTITION BY ds.deal_id, ds.state, ds.updated_date_utc ORDER BY ds.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.deal_states ds
    WHERE ds.deal_id IS NOT NULL 
      AND ds.state IS NOT NULL
      AND ds.updated_date_utc IS NOT NULL
      AND (ds._fivetran_deleted = FALSE OR ds._fivetran_deleted IS NULL)
  ),
  
  car_data AS (
    SELECT
      c.deal_id,
      UPPER(c.vin) as vin
    FROM bronze.leaseend_db_public.cars c
    WHERE c.deal_id IS NOT NULL AND c.vin IS NOT NULL
  ),
  credit_memo_vins AS (
    SELECT UPPER(t.custbody_leaseend_vinno) AS vin, MIN(DATE(t.trandate)) AS credit_memo_date
    FROM bronze.ns.transaction t
    WHERE t.abbrevtype = 'CREDITMEMO'
      AND t.custbody_leaseend_vinno IS NOT NULL
      AND LENGTH(t.custbody_leaseend_vinno) = 17
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  )
  
  SELECT
    dst.deal_key,
    CAST(DATE_FORMAT(dst.milestone_date_utc, 'yyyyMMdd') AS INT) AS milestone_date_key,
    CAST(DATE_FORMAT(dst.milestone_date_utc, 'HHmmss') AS INT) AS milestone_time_key,
    dst.deal_state_key,
    CAST(dst.user_key AS STRING) as user_key,
    
    -- Milestone classification based on deal state dimension
    CASE 
      WHEN dst.deal_state_key IN ('estimate', 'appointment') THEN 'initiation'
      WHEN dst.deal_state_key IN ('pending', 'structuring', 'sent_to_processor', 'approved', 'signatures', 'signed') THEN 'processing'
      WHEN dst.deal_state_key IN ('closing', 'soft_close', 'closed', 'funded', 'finalized') THEN 'completion'
      WHEN dst.deal_state_key IN ('booted', 'cancelled', 'declined') THEN 'cancellation'
      ELSE 'other'
    END as milestone_category,
    
    -- Workflow order from deal state dimension
    CASE dst.deal_state_key
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
    END as milestone_order,
    
    dst.state_sequence,
    
    -- Time-based measures
    CASE 
      WHEN dcd.creation_date_utc IS NOT NULL 
      THEN DATEDIFF(dst.milestone_date_utc, dcd.creation_date_utc)
      ELSE NULL
    END as days_since_creation,
    
    CASE 
      WHEN dst.previous_milestone_date IS NOT NULL 
      THEN DATEDIFF(dst.milestone_date_utc, dst.previous_milestone_date)
      ELSE NULL
    END as days_since_previous_milestone,
    
    CASE 
      WHEN dst.previous_milestone_date IS NOT NULL 
      THEN CAST((UNIX_TIMESTAMP(dst.milestone_date_utc) - UNIX_TIMESTAMP(dst.previous_milestone_date)) / 3600 AS INT)
      ELSE NULL
    END as hours_since_previous_milestone,
    
    -- Milestone flags
    CASE WHEN dst.deal_state_key = 'signed' THEN true ELSE false END as is_revenue_milestone,
    CASE WHEN dst.deal_state_key IN ('finalized', 'closed', 'funded') THEN true ELSE false END as is_completion_milestone,
    CASE WHEN dst.deal_state_key IN ('booted', 'cancelled', 'declined') THEN true ELSE false END as is_cancellation_milestone,
    
    CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END AS has_credit_memo,
    CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT) AS credit_memo_date_key,
    CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT) AS credit_memo_time_key,
    
    'bronze.leaseend_db_public.deal_states' as _source_table
    
  FROM deal_state_transitions dst
  LEFT JOIN deal_creation_dates dcd ON dst.deal_key = dcd.deal_id
  LEFT JOIN car_data cd ON dst.deal_key = cd.deal_id
  LEFT JOIN credit_memo_vins cmv ON cd.vin = cmv.vin
  WHERE dst.rn = 1 -- Only take the latest record for each unique combination

) AS source
ON target.deal_key = source.deal_key 
   AND target.milestone_date_key = source.milestone_date_key 
   AND target.deal_state_key = source.deal_state_key

-- Update existing milestones if timing measures change
WHEN MATCHED AND (
    COALESCE(target.days_since_creation, -1) != COALESCE(source.days_since_creation, -1) OR
    COALESCE(target.days_since_previous_milestone, -1) != COALESCE(source.days_since_previous_milestone, -1) OR
    COALESCE(target.hours_since_previous_milestone, -1) != COALESCE(source.hours_since_previous_milestone, -1) OR
    target.state_sequence != source.state_sequence
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
    target.has_credit_memo = source.has_credit_memo,
    target.credit_memo_date_key = source.credit_memo_date_key,
    target.credit_memo_time_key = source.credit_memo_time_key,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new milestones
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
    has_credit_memo,
    credit_memo_date_key,
    credit_memo_time_key,
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
    source.has_credit_memo,
    source.credit_memo_date_key,
    source.credit_memo_time_key,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 