-- models/dim/dim_employee.sql
-- Dimension table for employees, sourced only from Users (no BambooHR/account_user).

CREATE TABLE IF NOT EXISTS gold.finance.dim_employee (
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
COMMENT 'Dimension table storing employee information from User system only.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Upsert logic for dim_employee
MERGE INTO gold.finance.dim_employee AS target
USING (
  SELECT
    id AS employee_key,
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
    'bronze.leaseend_db_public.users' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM bronze.leaseend_db_public.users
  WHERE id IS NOT NULL
) AS source
ON target.employee_key = source.employee_key
WHEN MATCHED AND (
    target.email <> source.email OR
    target.name <> source.name OR
    target.nickname <> source.nickname OR
    target.phone_number <> source.phone_number OR
    target.twilio_number <> source.twilio_number OR
    target.created_at <> source.created_at OR
    target.updated_at <> source.updated_at OR
    target.on_vacation <> source.on_vacation OR
    target.auth0_roles <> source.auth0_roles OR
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