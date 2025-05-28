-- models/gold/dim/dim_deal_state.sql
-- Gold layer deal state dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_deal_state (
  deal_state_key STRING NOT NULL,
  deal_state_name STRING,
  deal_state_category STRING,
  is_active_state BOOLEAN,
  is_completed_state BOOLEAN,
  is_cancelled_state BOOLEAN,
  state_order INT,
  state_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer deal state dimension with business workflow classifications'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_deal_state AS target
USING (
  SELECT
    sds.deal_state_key,
    sds.deal_state_name,
    -- Categorize deal states into business groups
    CASE
      WHEN sds.deal_state_key IN ('new', 'in_progress', 'pending_approval', 'under_review') THEN 'In Progress'
      WHEN sds.deal_state_key IN ('completed', 'funded', 'closed') THEN 'Completed'
      WHEN sds.deal_state_key IN ('cancelled', 'rejected', 'withdrawn') THEN 'Cancelled'
      WHEN sds.deal_state_key = 'Unknown' THEN 'Unknown'
      ELSE 'Other'
    END AS deal_state_category,
    sds.is_active_state,
    sds.is_completed_state,
    sds.is_cancelled_state,
    -- Add ordering for workflow progression
    CASE sds.deal_state_key
      WHEN 'new' THEN 1
      WHEN 'in_progress' THEN 2
      WHEN 'pending_approval' THEN 3
      WHEN 'under_review' THEN 4
      WHEN 'approved' THEN 5
      WHEN 'funded' THEN 6
      WHEN 'completed' THEN 7
      WHEN 'closed' THEN 8
      WHEN 'cancelled' THEN 99
      WHEN 'rejected' THEN 98
      WHEN 'withdrawn' THEN 97
      ELSE 0
    END AS state_order,
    COALESCE(sds.deal_state_name, sds.deal_state_key, 'Unknown State') AS state_description,
    sds._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_deal_state sds
) AS source
ON target.deal_state_key = source.deal_state_key

WHEN MATCHED THEN
  UPDATE SET
    target.deal_state_name = source.deal_state_name,
    target.deal_state_category = source.deal_state_category,
    target.is_active_state = source.is_active_state,
    target.is_completed_state = source.is_completed_state,
    target.is_cancelled_state = source.is_cancelled_state,
    target.state_order = source.state_order,
    target.state_description = source.state_description,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    deal_state_key,
    deal_state_name,
    deal_state_category,
    is_active_state,
    is_completed_state,
    is_cancelled_state,
    state_order,
    state_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_state_key,
    source.deal_state_name,
    source.deal_state_category,
    source.is_active_state,
    source.is_completed_state,
    source.is_cancelled_state,
    source.state_order,
    source.state_description,
    source._source_table,
    source._load_timestamp
  ); 