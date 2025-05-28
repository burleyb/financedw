-- models/silver/dim/dim_pod.sql
-- Silver layer pod dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_pod (
  pod_key INT NOT NULL, -- Natural key from pods.id
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
COMMENT 'Silver layer pod dimension with commission rates and team configuration'
PARTITIONED BY (team_type)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_pod AS target
USING (
  WITH source_pods AS (
    SELECT
      p.id,
      p.name,
      p.color,
      p.manager_commission_rate,
      p.vsc_markup,
      p.setter_commission_rate,
      p.closer_commission_type,
      p.archived,
      p.special_commission_rate,
      p.special_commission_type,
      p.phone,
      p.vsc_multiplier,
      p.parent_pod_id,
      p.setter_commission_type,
      p.closer_commission_rate,
      p.team_type,
      p.manager_commission_type,
      p.us_states_object,
      p.problem_solver,
      p.created_at,
      p.updated_at,
      ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY p.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.pods p
    WHERE p.id IS NOT NULL
      AND p._fivetran_deleted = FALSE
  )
  SELECT
    sp.id AS pod_key,
    sp.name,
    sp.color,
    sp.manager_commission_rate,
    sp.vsc_markup,
    sp.setter_commission_rate,
    sp.closer_commission_type,
    sp.archived,
    sp.special_commission_rate,
    sp.special_commission_type,
    sp.phone,
    sp.vsc_multiplier,
    sp.parent_pod_id,
    sp.setter_commission_type,
    sp.closer_commission_rate,
    COALESCE(sp.team_type, 'Unknown') as team_type,
    sp.manager_commission_type,
    sp.us_states_object,
    sp.problem_solver,
    sp.created_at,
    sp.updated_at,
    'bronze.leaseend_db_public.pods' AS _source_table
  FROM source_pods sp
  WHERE sp.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    -1 as pod_key,
    'Unknown' as name,
    NULL as color,
    NULL as manager_commission_rate,
    NULL as vsc_markup,
    NULL as setter_commission_rate,
    NULL as closer_commission_type,
    false as archived,
    NULL as special_commission_rate,
    NULL as special_commission_type,
    NULL as phone,
    NULL as vsc_multiplier,
    NULL as parent_pod_id,
    NULL as setter_commission_type,
    NULL as closer_commission_rate,
    'Unknown' as team_type,
    NULL as manager_commission_type,
    NULL as us_states_object,
    false as problem_solver,
    CAST('1900-01-01 00:00:00' AS TIMESTAMP) as created_at,
    CAST('1900-01-01 00:00:00' AS TIMESTAMP) as updated_at,
    'system' AS _source_table
) AS source
ON target.pod_key = source.pod_key

-- Update existing pods if any attributes change
WHEN MATCHED AND (
    COALESCE(target.name, '') <> COALESCE(source.name, '') OR
    COALESCE(target.color, '') <> COALESCE(source.color, '') OR
    COALESCE(target.manager_commission_rate, 0) <> COALESCE(source.manager_commission_rate, 0) OR
    COALESCE(target.vsc_markup, 0) <> COALESCE(source.vsc_markup, 0) OR
    COALESCE(target.setter_commission_rate, 0) <> COALESCE(source.setter_commission_rate, 0) OR
    COALESCE(target.closer_commission_type, '') <> COALESCE(source.closer_commission_type, '') OR
    COALESCE(target.archived, false) <> COALESCE(source.archived, false) OR
    COALESCE(target.updated_at, CAST('1900-01-01' AS TIMESTAMP)) <> COALESCE(source.updated_at, CAST('1900-01-01' AS TIMESTAMP))
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
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new pods
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
    CURRENT_TIMESTAMP()
  ); 