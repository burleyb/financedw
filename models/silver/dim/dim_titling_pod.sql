-- models/silver/dim/dim_titling_pod.sql
-- Silver layer titling pod dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_titling_pod (
  titling_pod_key INT NOT NULL, -- Natural key from titling_pods.id
  name STRING,
  color STRING,
  us_states STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  problem_solver BOOLEAN,
  archived BOOLEAN,
  us_states_object STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer titling pod dimension for title and registration processing teams'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_titling_pod AS target
USING (
  WITH source_titling_pods AS (
    SELECT
      tp.id,
      tp.name,
      tp.color,
      tp.us_states,
      tp.created_at,
      tp.updated_at,
      tp.problem_solver,
      tp.archived,
      tp.us_states_object,
      ROW_NUMBER() OVER (PARTITION BY tp.id ORDER BY tp.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.titling_pods tp
    WHERE tp.id IS NOT NULL
      AND tp._fivetran_deleted = FALSE
  )
  SELECT
    stp.id AS titling_pod_key,
    stp.name,
    stp.color,
    stp.us_states,
    stp.created_at,
    stp.updated_at,
    stp.problem_solver,
    stp.archived,
    stp.us_states_object,
    'bronze.leaseend_db_public.titling_pods' AS _source_table
  FROM source_titling_pods stp
  WHERE stp.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    -1 as titling_pod_key,
    'Unknown' as name,
    NULL as color,
    NULL as us_states,
    CAST('1900-01-01 00:00:00' AS TIMESTAMP) as created_at,
    CAST('1900-01-01 00:00:00' AS TIMESTAMP) as updated_at,
    false as problem_solver,
    false as archived,
    NULL as us_states_object,
    'system' AS _source_table
) AS source
ON target.titling_pod_key = source.titling_pod_key

-- Update existing titling pods if any attributes change
WHEN MATCHED AND (
    COALESCE(target.name, '') <> COALESCE(source.name, '') OR
    COALESCE(target.color, '') <> COALESCE(source.color, '') OR
    COALESCE(target.us_states, '') <> COALESCE(source.us_states, '') OR
    COALESCE(target.problem_solver, false) <> COALESCE(source.problem_solver, false) OR
    COALESCE(target.archived, false) <> COALESCE(source.archived, false) OR
    COALESCE(target.updated_at, CAST('1900-01-01' AS TIMESTAMP)) <> COALESCE(source.updated_at, CAST('1900-01-01' AS TIMESTAMP))
  ) THEN
  UPDATE SET
    target.name = source.name,
    target.color = source.color,
    target.us_states = source.us_states,
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target.problem_solver = source.problem_solver,
    target.archived = source.archived,
    target.us_states_object = source.us_states_object,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new titling pods
WHEN NOT MATCHED THEN
  INSERT (
    titling_pod_key,
    name,
    color,
    us_states,
    created_at,
    updated_at,
    problem_solver,
    archived,
    us_states_object,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.titling_pod_key,
    source.name,
    source.color,
    source.us_states,
    source.created_at,
    source.updated_at,
    source.problem_solver,
    source.archived,
    source.us_states_object,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 