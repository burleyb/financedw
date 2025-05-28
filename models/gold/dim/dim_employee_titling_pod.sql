-- models/gold/dim/dim_employee_titling_pod.sql
-- Gold layer employee titling pod bridge table with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_employee_titling_pod (
  employee_key STRING NOT NULL,
  titling_pod_key INT NOT NULL,
  relationship_status STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer employee titling pod bridge table with relationship status'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_employee_titling_pod AS target
USING (
  SELECT
    setp.employee_key,
    setp.titling_pod_key,
    'Active' AS relationship_status,  -- All relationships in the bridge are active
    setp._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_employee_titling_pod setp
) AS source
ON target.employee_key = source.employee_key AND target.titling_pod_key = source.titling_pod_key

WHEN MATCHED THEN
  UPDATE SET
    target.relationship_status = source.relationship_status,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    employee_key,
    titling_pod_key,
    relationship_status,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employee_key,
    source.titling_pod_key,
    source.relationship_status,
    source._source_table,
    source._load_timestamp
  ); 