-- models/relationships/dim_employee_titling_pod.sql
-- Bridge table for employee-to-titling pod relationships

CREATE TABLE IF NOT EXISTS gold.finance.dim_employee_titling_pod (
  employee_key STRING NOT NULL,      -- FK to dim_employee (users.id)
  titling_pod_key INT NOT NULL,      -- FK to dim_titling_pod (titling_pods.id)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Bridge table mapping employees to titling pods.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

MERGE INTO gold.finance.dim_employee_titling_pod AS target
USING (
  SELECT
    tpu.user_id AS employee_key,
    tpu.titling_pod_id AS titling_pod_key,
    'bronze.leaseend_db_public.titling_pods_users' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM bronze.leaseend_db_public.titling_pods_users tpu
  JOIN gold.finance.dim_employee e ON tpu.user_id = e.employee_key
  JOIN gold.finance.dim_titling_pod tp ON tpu.titling_pod_id = tp.titling_pod_key
  WHERE tpu.user_id IS NOT NULL AND tpu.titling_pod_id IS NOT NULL
) AS source
ON target.employee_key = source.employee_key AND target.titling_pod_key = source.titling_pod_key
WHEN MATCHED THEN
  UPDATE SET
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    employee_key,
    titling_pod_key,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employee_key,
    source.titling_pod_key,
    source._source_table,
    source._load_timestamp
  ); 