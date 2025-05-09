-- models/relationships/dim_employee_pod.sql
-- Bridge table for employee-to-pod relationships

CREATE TABLE IF NOT EXISTS gold.finance.dim_employee_pod (
  employee_key STRING NOT NULL, -- FK to dim_employee (users.id)
  pod_key INT NOT NULL,         -- FK to dim_pod (pods.id)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Bridge table mapping employees to pods.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

MERGE INTO gold.finance.dim_employee_pod AS target
USING (
  SELECT
    pu.user_id AS employee_key,
    pu.pod_id AS pod_key,
    'bronze.leaseend_db_public.pods_users' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM bronze.leaseend_db_public.pods_users pu
  JOIN gold.finance.dim_employee e ON pu.user_id = e.employee_key
  JOIN gold.finance.dim_pod p ON pu.pod_id = p.pod_key
  WHERE pu.user_id IS NOT NULL AND pu.pod_id IS NOT NULL
) AS source
ON target.employee_key = source.employee_key AND target.pod_key = source.pod_key
WHEN MATCHED THEN
  UPDATE SET
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    employee_key,
    pod_key,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employee_key,
    source.pod_key,
    source._source_table,
    source._load_timestamp
  ); 