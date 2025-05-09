-- models/dim/dim_titling_pod.sql
-- Dimension table for titling pods

CREATE TABLE IF NOT EXISTS gold.finance.dim_titling_pod (
  titling_pod_key INT NOT NULL,
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
COMMENT 'Dimension table for titling pods.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

MERGE INTO gold.finance.dim_titling_pod AS target
USING (
  SELECT
    id AS titling_pod_key,
    name,
    color,
    us_states,
    created_at,
    updated_at,
    problem_solver,
    archived,
    us_states_object,
    'bronze.leaseend_db_public.titling_pods' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM bronze.leaseend_db_public.titling_pods
  WHERE id IS NOT NULL
) AS source
ON target.titling_pod_key = source.titling_pod_key
WHEN MATCHED AND (
    target.name <> source.name OR
    target.color <> source.color OR
    target.us_states <> source.us_states OR
    target.created_at <> source.created_at OR
    target.updated_at <> source.updated_at OR
    target.problem_solver <> source.problem_solver OR
    target.archived <> source.archived OR
    target.us_states_object <> source.us_states_object
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
    target._load_timestamp = source._load_timestamp
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
    source._load_timestamp
  ); 