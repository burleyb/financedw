-- models/dim/dim_pod.sql
-- Dimension table for pods

CREATE TABLE IF NOT EXISTS gold.finance.dim_pod (
  pod_key INT NOT NULL,
  name STRING,
  color STRING,
  manager_commission_rate DECIMAL(8,4),
  vsc_markup INT,
  setter_commission_rate DECIMAL(8,4),
  closer_commission_type STRING,
  archived BOOLEAN,
  special_commission_rate DECIMAL(8,4),
  special_commission_type STRING,
  phone STRING,
  vsc_multiplier DECIMAL(8,2),
  parent_pod_id INT,
  setter_commission_type STRING,
  closer_commission_rate DECIMAL(8,4),
  team_type STRING,
  manager_commission_type STRING,
  us_states_object STRING,
  problem_solver BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table for pods.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

MERGE INTO gold.finance.dim_pod AS target
USING (
  SELECT
    id AS pod_key,
    name,
    color,
    manager_commission_rate,
    vsc_markup,
    setter_commission_rate,
    closer_commission_type,
    archived,
    special_commission_rate,
    special_commission_type,
    phone,
    vsc_multiplier,
    parent_pod_id,
    setter_commission_type,
    closer_commission_rate,
    team_type,
    manager_commission_type,
    us_states_object,
    problem_solver,
    created_at,
    updated_at,
    'bronze.leaseend_db_public.pods' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM bronze.leaseend_db_public.pods
  WHERE id IS NOT NULL
) AS source
ON target.pod_key = source.pod_key
WHEN MATCHED AND (
    target.name <> source.name OR
    target.color <> source.color OR
    target.manager_commission_rate <> source.manager_commission_rate OR
    target.vsc_markup <> source.vsc_markup OR
    target.setter_commission_rate <> source.setter_commission_rate OR
    target.closer_commission_type <> source.closer_commission_type OR
    target.archived <> source.archived OR
    target.special_commission_rate <> source.special_commission_rate OR
    target.special_commission_type <> source.special_commission_type OR
    target.phone <> source.phone OR
    target.vsc_multiplier <> source.vsc_multiplier OR
    target.parent_pod_id <> source.parent_pod_id OR
    target.setter_commission_type <> source.setter_commission_type OR
    target.closer_commission_rate <> source.closer_commission_rate OR
    target.team_type <> source.team_type OR
    target.manager_commission_type <> source.manager_commission_type OR
    target.us_states_object <> source.us_states_object OR
    target.problem_solver <> source.problem_solver OR
    target.created_at <> source.created_at OR
    target.updated_at <> source.updated_at
  ) THEN
  UPDATE SET
    target.name = source.name,
    target.color = source.color,
    target.manager_commission_rate = source.manager_commission_rate,
    target.vsc_markup = source.vsc_markup,
    target.setter_commission_rate = source.setter_commission_rate,
    target.closer_commission_type = source.closer_commission_type,
    target.archived = source.archived,
    target.special_commission_rate = source.special_commission_rate,
    target.special_commission_type = source.special_commission_type,
    target.phone = source.phone,
    target.vsc_multiplier = source.vsc_multiplier,
    target.parent_pod_id = source.parent_pod_id,
    target.setter_commission_type = source.setter_commission_type,
    target.closer_commission_rate = source.closer_commission_rate,
    target.team_type = source.team_type,
    target.manager_commission_type = source.manager_commission_type,
    target.us_states_object = source.us_states_object,
    target.problem_solver = source.problem_solver,
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    pod_key,
    name,
    color,
    manager_commission_rate,
    vsc_markup,
    setter_commission_rate,
    closer_commission_type,
    archived,
    special_commission_rate,
    special_commission_type,
    phone,
    vsc_multiplier,
    parent_pod_id,
    setter_commission_type,
    closer_commission_rate,
    team_type,
    manager_commission_type,
    us_states_object,
    problem_solver,
    created_at,
    updated_at,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.pod_key,
    source.name,
    source.color,
    source.manager_commission_rate,
    source.vsc_markup,
    source.setter_commission_rate,
    source.closer_commission_type,
    source.archived,
    source.special_commission_rate,
    source.special_commission_type,
    source.phone,
    source.vsc_multiplier,
    source.parent_pod_id,
    source.setter_commission_type,
    source.closer_commission_rate,
    source.team_type,
    source.manager_commission_type,
    source.us_states_object,
    source.problem_solver,
    source.created_at,
    source.updated_at,
    source._source_table,
    source._load_timestamp
  ); 