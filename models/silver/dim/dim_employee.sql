-- models/silver/dim/dim_employee.sql
-- Silver layer employee dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_employee (
  employee_key STRING NOT NULL, -- Natural key from users.id
  email STRING,
  name STRING,
  nickname STRING,
  phone_number STRING,
  twilio_number STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  on_vacation BOOLEAN,
  auth0_roles STRING,
  location STRING,
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Silver layer employee dimension from users table'
PARTITIONED BY (location)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Upsert logic for dim_employee
MERGE INTO silver.finance.dim_employee AS target
USING (
  SELECT
    CAST(id AS STRING) AS employee_key,
    COALESCE(email, 'Unknown') AS email,
    COALESCE(name, 'Unknown') AS name,
    COALESCE(nickname, 'Unknown') AS nickname,
    phone_number,
    twilio_number,
    CASE 
      WHEN created_at >= '1900-01-01' AND created_at <= '2037-12-31' THEN created_at
      ELSE NULL
    END AS created_at,
    CASE 
      WHEN updated_at >= '1900-01-01' AND updated_at <= '2037-12-31' THEN updated_at
      ELSE NULL
    END AS updated_at,
    COALESCE(on_vacation, false) AS on_vacation,
    auth0_roles,
    COALESCE(location, 'Unknown') AS location,
    'bronze.leaseend_db_public.users' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM bronze.leaseend_db_public.users
  WHERE id IS NOT NULL
    AND _fivetran_deleted = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
) AS source
ON target.employee_key = source.employee_key
WHEN MATCHED AND (
    target.email <> source.email OR
    target.name <> source.name OR
    target.nickname <> source.nickname OR
    COALESCE(target.phone_number, '') <> COALESCE(source.phone_number, '') OR
    COALESCE(target.twilio_number, '') <> COALESCE(source.twilio_number, '') OR
    COALESCE(target.created_at, '1900-01-01') <> COALESCE(source.created_at, '1900-01-01') OR
    COALESCE(target.updated_at, '1900-01-01') <> COALESCE(source.updated_at, '1900-01-01') OR
    target.on_vacation <> source.on_vacation OR
    COALESCE(target.auth0_roles, '') <> COALESCE(source.auth0_roles, '') OR
    target.location <> source.location
  ) THEN
  UPDATE SET
    target.email = source.email,
    target.name = source.name,
    target.nickname = source.nickname,
    target.phone_number = source.phone_number,
    target.twilio_number = source.twilio_number,
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target.on_vacation = source.on_vacation,
    target.auth0_roles = source.auth0_roles,
    target.location = source.location,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    employee_key,
    email,
    name,
    nickname,
    phone_number,
    twilio_number,
    created_at,
    updated_at,
    on_vacation,
    auth0_roles,
    location,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employee_key,
    source.email,
    source.name,
    source.nickname,
    source.phone_number,
    source.twilio_number,
    source.created_at,
    source.updated_at,
    source.on_vacation,
    source.auth0_roles,
    source.location,
    source._source_table,
    source._load_timestamp
  );

-- Ensure 'Unknown' employee exists for handling NULLs
MERGE INTO silver.finance.dim_employee AS target
USING (
  SELECT 
    'unknown' as employee_key, 
    'Unknown' as email,
    'Unknown' as name,
    'Unknown' as nickname,
    NULL as phone_number,
    NULL as twilio_number,
    NULL as created_at,
    NULL as updated_at,
    false as on_vacation,
    NULL as auth0_roles,
    'Unknown' as location,
    'system' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
) AS source
ON target.employee_key = source.employee_key
WHEN NOT MATCHED THEN 
  INSERT (
    employee_key, email, name, nickname, phone_number, twilio_number,
    created_at, updated_at, on_vacation, auth0_roles, location,
    _source_table, _load_timestamp
  )
  VALUES (
    source.employee_key, source.email, source.name, source.nickname, 
    source.phone_number, source.twilio_number, source.created_at, 
    source.updated_at, source.on_vacation, source.auth0_roles, 
    source.location, source._source_table, source._load_timestamp
  ); 