-- models/silver/dim/dim_employee.sql
-- Silver layer employee dimension table reading from bronze sources

DROP TABLE IF EXISTS silver.finance.dim_employee;

CREATE TABLE IF NOT EXISTS silver.finance.dim_employee (
  employee_key STRING NOT NULL, -- Natural key from users.id
  email STRING,
  name STRING,
  nickname STRING,
  phone_number STRING,
  twilio_number STRING,
  created_date_key INT, -- FK to dim_date
  created_time_key INT, -- FK to dim_time
  updated_date_key INT, -- FK to dim_date
  updated_time_key INT, -- FK to dim_time
  on_vacation BOOLEAN,
  auth0_roles STRING,
  location STRING,
  -- BambooHR fields
  employee_id INT, -- BambooHR employee ID
  original_hire_date_key INT, -- FK to dim_date for first hire date if rehired
  hire_date_key INT, -- FK to dim_date for current/most recent hire date
  termination_date_key INT, -- FK to dim_date for termination date (NULL if active)
  employment_status STRING, -- Full-Time, Part-Time, etc.
  employment_status_date_key INT, -- FK to dim_date for last employment status change
  job_title STRING, -- Current job title
  department STRING, -- Department
  reporting_to STRING, -- Manager name
  state STRING, -- State where employee works
  fte STRING, -- Full-time equivalent value
  is_active BOOLEAN, -- Derived field: TRUE if no termination_date or termination_date > current_date
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Silver layer employee dimension combining users and BambooHR data'
PARTITIONED BY (location)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Upsert logic for dim_employee
MERGE INTO silver.finance.dim_employee AS target
USING (
  -- Join users with BambooHR data
  WITH users_data AS (
    SELECT
      CAST(u.id AS STRING) AS employee_key,
      COALESCE(u.email, 'Unknown') AS email,
      COALESCE(u.name, 'Unknown') AS name,
      COALESCE(u.nickname, 'Unknown') AS nickname,
      u.phone_number,
      u.twilio_number,
      -- Convert created_at to date_key and time_key
      CASE
        WHEN u.created_at IS NOT NULL THEN CAST(DATE_FORMAT(u.created_at, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS created_date_key,
      CASE
        WHEN u.created_at IS NOT NULL THEN CAST(DATE_FORMAT(u.created_at, 'HHmmss') AS INT)
        ELSE NULL
      END AS created_time_key,
      -- Convert updated_at to date_key and time_key
      CASE
        WHEN u.updated_at IS NOT NULL THEN CAST(DATE_FORMAT(u.updated_at, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS updated_date_key,
      CASE
        WHEN u.updated_at IS NOT NULL THEN CAST(DATE_FORMAT(u.updated_at, 'HHmmss') AS INT)
        ELSE NULL
      END AS updated_time_key,
      COALESCE(u.on_vacation, false) AS on_vacation,
      u.auth0_roles,
      COALESCE(u.location, 'Unknown') AS location
    FROM bronze.leaseend_db_public.users u
    WHERE u.id IS NOT NULL
      AND u._fivetran_deleted = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY u.updated_at DESC) = 1
  ),
  bamboo_data AS (
    SELECT
      e.id AS employee_id,
      au.email,
      -- Convert date fields to date_keys
      CASE
        WHEN e.original_hire_date IS NOT NULL THEN CAST(DATE_FORMAT(e.original_hire_date, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS original_hire_date_key,
      CASE
        WHEN e.hire_date IS NOT NULL THEN CAST(DATE_FORMAT(e.hire_date, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS hire_date_key,
      CASE
        WHEN e.termination_date IS NOT NULL THEN CAST(DATE_FORMAT(e.termination_date, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS termination_date_key,
      e.employment_status,
      CASE
        WHEN e.employment_status_date IS NOT NULL THEN CAST(DATE_FORMAT(e.employment_status_date, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS employment_status_date_key,
      e.job_title,
      e.department,
      e.reporting_to,
      e.state,
      e.fte,
      CASE WHEN e.termination_date IS NULL OR e.termination_date > CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_active
    FROM bronze.bamboohr.employee e
    LEFT JOIN bronze.bamboohr.account_user au ON e.id = au.employee_id
    WHERE e._fivetran_deleted = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.id ORDER BY e.last_changed DESC) = 1
  )
  SELECT
    u.employee_key,
    COALESCE(u.email, b.email, 'Unknown') AS email,
    u.name,
    u.nickname,
    u.phone_number,
    u.twilio_number,
    u.created_date_key,
    u.created_time_key,
    u.updated_date_key,
    u.updated_time_key,
    u.on_vacation,
    u.auth0_roles,
    u.location,
    b.employee_id,
    b.original_hire_date_key,
    b.hire_date_key,
    b.termination_date_key,
    b.employment_status,
    b.employment_status_date_key,
    b.job_title,
    b.department,
    b.reporting_to,
    b.state,
    b.fte,
    b.is_active,
    CASE
      WHEN b.employee_id IS NOT NULL THEN 'bronze.bamboohr.employee,bronze.leaseend_db_public.users'
      ELSE 'bronze.leaseend_db_public.users'
    END AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM users_data u
  LEFT JOIN bamboo_data b ON LOWER(u.email) = LOWER(b.email)
  
  UNION ALL
  
  -- Include BambooHR employees that don't exist in users table
  SELECT
    CAST(b.employee_id AS STRING) AS employee_key,
    COALESCE(b.email, 'Unknown') AS email,
    CONCAT(b.first_name, ' ', b.last_name) AS name,
    NULL AS nickname,
    NULL AS phone_number,
    NULL AS twilio_number,
    -- Convert hire_date to date_key for created_date_key
    b.hire_date_key AS created_date_key,
    -- Default time for hire_date is start of day
    CAST(0 AS INT) AS created_time_key,
    NULL AS updated_date_key,
    NULL AS updated_time_key,
    FALSE AS on_vacation,
    NULL AS auth0_roles,
    COALESCE(b.state, 'Unknown') AS location,
    b.employee_id,
    b.original_hire_date_key,
    b.hire_date_key,
    b.termination_date_key,
    b.employment_status,
    b.employment_status_date_key,
    b.job_title,
    b.department,
    b.reporting_to,
    b.state,
    b.fte,
    b.is_active,
    'bronze.bamboohr.employee' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM (
    SELECT
      e.id AS employee_id,
      au.email,
      e.first_name,
      e.last_name,
      e.hire_date,
      -- Convert date fields to date_keys
      CASE
        WHEN e.original_hire_date IS NOT NULL THEN CAST(DATE_FORMAT(e.original_hire_date, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS original_hire_date_key,
      CASE
        WHEN e.hire_date IS NOT NULL THEN CAST(DATE_FORMAT(e.hire_date, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS hire_date_key,
      CASE
        WHEN e.termination_date IS NOT NULL THEN CAST(DATE_FORMAT(e.termination_date, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS termination_date_key,
      e.employment_status,
      CASE
        WHEN e.employment_status_date IS NOT NULL THEN CAST(DATE_FORMAT(e.employment_status_date, 'yyyyMMdd') AS INT)
        ELSE NULL
      END AS employment_status_date_key,
      e.job_title,
      e.department,
      e.reporting_to,
      e.state,
      e.fte,
      CASE WHEN e.termination_date IS NULL OR e.termination_date > CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_active
    FROM bronze.bamboohr.employee e
    LEFT JOIN bronze.bamboohr.account_user au ON e.id = au.employee_id
    WHERE e._fivetran_deleted = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.id ORDER BY e.last_changed DESC) = 1
  ) b
  WHERE NOT EXISTS (
    SELECT 1 
    FROM users_data u 
    WHERE LOWER(u.email) = LOWER(b.email) AND b.email IS NOT NULL
  )
) AS source
ON target.employee_key = source.employee_key
WHEN MATCHED AND (
    target.email <> source.email OR
    target.name <> source.name OR
    target.nickname <> source.nickname OR
    COALESCE(target.phone_number, '') <> COALESCE(source.phone_number, '') OR
    COALESCE(target.twilio_number, '') <> COALESCE(source.twilio_number, '') OR
    COALESCE(target.created_date_key, -1) <> COALESCE(source.created_date_key, -1) OR
    COALESCE(target.created_time_key, -1) <> COALESCE(source.created_time_key, -1) OR
    COALESCE(target.updated_date_key, -1) <> COALESCE(source.updated_date_key, -1) OR
    COALESCE(target.updated_time_key, -1) <> COALESCE(source.updated_time_key, -1) OR
    target.on_vacation <> source.on_vacation OR
    COALESCE(target.auth0_roles, '') <> COALESCE(source.auth0_roles, '') OR
    target.location <> source.location OR
    COALESCE(target.employee_id, -1) <> COALESCE(source.employee_id, -1) OR
    COALESCE(target.original_hire_date_key, -1) <> COALESCE(source.original_hire_date_key, -1) OR
    COALESCE(target.hire_date_key, -1) <> COALESCE(source.hire_date_key, -1) OR
    COALESCE(target.termination_date_key, -1) <> COALESCE(source.termination_date_key, -1) OR
    COALESCE(target.employment_status, '') <> COALESCE(source.employment_status, '') OR
    COALESCE(target.employment_status_date_key, -1) <> COALESCE(source.employment_status_date_key, -1) OR
    COALESCE(target.job_title, '') <> COALESCE(source.job_title, '') OR
    COALESCE(target.department, '') <> COALESCE(source.department, '') OR
    COALESCE(target.reporting_to, '') <> COALESCE(source.reporting_to, '') OR
    COALESCE(target.state, '') <> COALESCE(source.state, '') OR
    COALESCE(target.fte, '') <> COALESCE(source.fte, '') OR
    COALESCE(target.is_active, FALSE) <> COALESCE(source.is_active, FALSE)
  ) THEN
  UPDATE SET
    target.email = source.email,
    target.name = source.name,
    target.nickname = source.nickname,
    target.phone_number = source.phone_number,
    target.twilio_number = source.twilio_number,
    target.created_date_key = source.created_date_key,
    target.created_time_key = source.created_time_key,
    target.updated_date_key = source.updated_date_key,
    target.updated_time_key = source.updated_time_key,
    target.on_vacation = source.on_vacation,
    target.auth0_roles = source.auth0_roles,
    target.location = source.location,
    target.employee_id = source.employee_id,
    target.original_hire_date_key = source.original_hire_date_key,
    target.hire_date_key = source.hire_date_key,
    target.termination_date_key = source.termination_date_key,
    target.employment_status = source.employment_status,
    target.employment_status_date_key = source.employment_status_date_key,
    target.job_title = source.job_title,
    target.department = source.department,
    target.reporting_to = source.reporting_to,
    target.state = source.state,
    target.fte = source.fte,
    target.is_active = source.is_active,
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
    created_date_key,
    created_time_key,
    updated_date_key,
    updated_time_key,
    on_vacation,
    auth0_roles,
    location,
    employee_id,
    original_hire_date_key,
    hire_date_key,
    termination_date_key,
    employment_status,
    employment_status_date_key,
    job_title,
    department,
    reporting_to,
    state,
    fte,
    is_active,
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
    source.created_date_key,
    source.created_time_key,
    source.updated_date_key,
    source.updated_time_key,
    source.on_vacation,
    source.auth0_roles,
    source.location,
    source.employee_id,
    source.original_hire_date_key,
    source.hire_date_key,
    source.termination_date_key,
    source.employment_status,
    source.employment_status_date_key,
    source.job_title,
    source.department,
    source.reporting_to,
    source.state,
    source.fte,
    source.is_active,
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
    NULL as created_date_key,
    NULL as created_time_key,
    NULL as updated_date_key,
    NULL as updated_time_key,
    false as on_vacation,
    NULL as auth0_roles,
    'Unknown' as location,
    NULL as employee_id,
    NULL as original_hire_date_key,
    NULL as hire_date_key,
    NULL as termination_date_key,
    'Unknown' as employment_status,
    NULL as employment_status_date_key,
    'Unknown' as job_title,
    'Unknown' as department,
    'Unknown' as reporting_to,
    'Unknown' as state,
    NULL as fte,
    false as is_active,
    'system' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
) AS source
ON target.employee_key = source.employee_key
WHEN NOT MATCHED THEN 
  INSERT (
    employee_key, email, name, nickname, phone_number, twilio_number,
    created_date_key, created_time_key, 
    updated_date_key, updated_time_key, on_vacation, auth0_roles, location,
    employee_id, original_hire_date_key, hire_date_key, termination_date_key,
    employment_status, employment_status_date_key, job_title, department,
    reporting_to, state, fte, is_active, _source_table, _load_timestamp
  )
  VALUES (
    source.employee_key, source.email, source.name, source.nickname, 
    source.phone_number, source.twilio_number,  
    source.created_date_key, source.created_time_key,
    source.updated_date_key, source.updated_time_key, source.on_vacation, 
    source.auth0_roles, source.location, source.employee_id, 
    source.original_hire_date_key, source.hire_date_key, source.termination_date_key, 
    source.employment_status, source.employment_status_date_key, source.job_title, 
    source.department, source.reporting_to, source.state, source.fte, 
    source.is_active, source._source_table, source._load_timestamp
  ); 