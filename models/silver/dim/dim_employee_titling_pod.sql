-- models/silver/dim/dim_employee_titling_pod.sql
-- Silver layer bridge table for employee-to-titling pod relationships reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_employee_titling_pod (
  employee_key STRING NOT NULL,      -- FK to dim_employee (users.id)
  titling_pod_key INT NOT NULL,      -- FK to dim_titling_pod (titling_pods.id)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer bridge table mapping employees to titling pods'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_employee_titling_pod AS target
USING (
  WITH source_relationships AS (
    SELECT
      tpu.user_id,
      tpu.titling_pod_id,
      ROW_NUMBER() OVER (PARTITION BY tpu.user_id, tpu.titling_pod_id ORDER BY tpu.user_id) as rn
    FROM bronze.leaseend_db_public.titling_pods_users tpu
    WHERE tpu.user_id IS NOT NULL 
      AND tpu.titling_pod_id IS NOT NULL
      AND tpu._fivetran_deleted = FALSE
  )
  SELECT
    CAST(sr.user_id AS STRING) AS employee_key,
    sr.titling_pod_id AS titling_pod_key,
    'bronze.leaseend_db_public.titling_pods_users' AS _source_table
  FROM source_relationships sr
  WHERE sr.rn = 1
) AS source
ON target.employee_key = source.employee_key AND target.titling_pod_key = source.titling_pod_key

-- Update existing relationships (mainly for metadata)
WHEN MATCHED THEN
  UPDATE SET
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new relationships
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
    CURRENT_TIMESTAMP()
  ); 