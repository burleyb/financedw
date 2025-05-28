-- models/silver/dim/dim_employee_pod.sql
-- Silver layer bridge table for employee-to-pod relationships reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_employee_pod (
  employee_key STRING NOT NULL, -- FK to dim_employee (users.id)
  pod_key INT NOT NULL,         -- FK to dim_pod (pods.id)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer bridge table mapping employees to pods'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_employee_pod AS target
USING (
  WITH source_relationships AS (
    SELECT
      pu.user_id,
      pu.pod_id,
      ROW_NUMBER() OVER (PARTITION BY pu.user_id, pu.pod_id ORDER BY pu.user_id) as rn
    FROM bronze.leaseend_db_public.pods_users pu
    WHERE pu.user_id IS NOT NULL 
      AND pu.pod_id IS NOT NULL
      AND pu._fivetran_deleted = FALSE
  )
  SELECT
    CAST(sr.user_id AS STRING) AS employee_key,
    sr.pod_id AS pod_key,
    'bronze.leaseend_db_public.pods_users' AS _source_table
  FROM source_relationships sr
  WHERE sr.rn = 1
) AS source
ON target.employee_key = source.employee_key AND target.pod_key = source.pod_key

-- Update existing relationships (mainly for metadata)
WHEN MATCHED THEN
  UPDATE SET
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new relationships
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
    CURRENT_TIMESTAMP()
  ); 