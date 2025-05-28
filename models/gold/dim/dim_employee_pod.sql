-- models/gold/dim/dim_employee_pod.sql
-- Gold layer employee pod bridge table with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_employee_pod (
  employee_key STRING NOT NULL,
  pod_key INT NOT NULL,
  relationship_status STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer employee pod bridge table with relationship status'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_employee_pod AS target
USING (
  SELECT
    sep.employee_key,
    sep.pod_key,
    'Active' AS relationship_status,  -- All relationships in the bridge are active
    sep._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_employee_pod sep
) AS source
ON target.employee_key = source.employee_key AND target.pod_key = source.pod_key

WHEN MATCHED THEN
  UPDATE SET
    target.relationship_status = source.relationship_status,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    employee_key,
    pod_key,
    relationship_status,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employee_key,
    source.pod_key,
    source.relationship_status,
    source._source_table,
    source._load_timestamp
  ); 