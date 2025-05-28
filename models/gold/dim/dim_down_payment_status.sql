-- models/gold/dim/dim_down_payment_status.sql
-- Gold layer down payment status dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_down_payment_status (
  down_payment_status_key STRING NOT NULL,
  down_payment_status_description STRING,
  cash_flow_impact STRING,
  risk_indicator STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer down payment status dimension with cash flow and risk analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_down_payment_status AS target
USING (
  SELECT
    sdps.down_payment_status_key,
    sdps.down_payment_status_description,
    
    -- Cash flow impact assessment
    CASE
      WHEN sdps.down_payment_status_key = 'PAID' THEN 'Positive Cash Flow'
      WHEN sdps.down_payment_status_key = 'NOT_REQUIRED' THEN 'Neutral Cash Flow'
      WHEN sdps.down_payment_status_key = 'WAIVED' THEN 'Neutral Cash Flow'
      WHEN sdps.down_payment_status_key = 'PARTIAL' THEN 'Partial Cash Flow'
      WHEN sdps.down_payment_status_key = 'PENDING' THEN 'Delayed Cash Flow'
      ELSE 'Unknown'
    END AS cash_flow_impact,
    
    -- Risk indicator for deal completion
    CASE
      WHEN sdps.down_payment_status_key = 'PAID' THEN 'Low Risk'
      WHEN sdps.down_payment_status_key = 'NOT_REQUIRED' THEN 'Low Risk'
      WHEN sdps.down_payment_status_key = 'WAIVED' THEN 'Medium Risk'
      WHEN sdps.down_payment_status_key = 'PARTIAL' THEN 'Medium Risk'
      WHEN sdps.down_payment_status_key = 'PENDING' THEN 'High Risk'
      ELSE 'Unknown Risk'
    END AS risk_indicator,
    
    sdps._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_down_payment_status sdps
) AS source
ON target.down_payment_status_key = source.down_payment_status_key

WHEN MATCHED THEN
  UPDATE SET
    target.down_payment_status_description = source.down_payment_status_description,
    target.cash_flow_impact = source.cash_flow_impact,
    target.risk_indicator = source.risk_indicator,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    down_payment_status_key,
    down_payment_status_description,
    cash_flow_impact,
    risk_indicator,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.down_payment_status_key,
    source.down_payment_status_description,
    source.cash_flow_impact,
    source.risk_indicator,
    source._source_table,
    source._load_timestamp
  ); 