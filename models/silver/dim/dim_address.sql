-- models/silver/dim/dim_address.sql
-- Silver layer address dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_address (
  address_key STRING NOT NULL, -- Composite key based on address components
  address_line STRING,
  address_line_2 STRING,
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  residence_type STRING,
  years_at_home INT,
  months_at_home INT,
  time_zone STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer address dimension storing distinct addresses'
PARTITIONED BY (state)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_address AS target
USING (
  WITH source_addresses AS (
    SELECT DISTINCT
      a.address_line,
      a.address_line_2,
      a.city,
      a.state,
      a.zip,
      a.county,
      a.residence_type,
      a.years_at_home,
      a.months_at_home,
      a.time_zone,
      ROW_NUMBER() OVER (
        PARTITION BY 
          COALESCE(a.address_line, ''),
          COALESCE(a.address_line_2, ''),
          COALESCE(a.city, ''),
          COALESCE(a.state, ''),
          COALESCE(a.zip, '')
        ORDER BY a.updated_at DESC
      ) as rn
    FROM bronze.leaseend_db_public.addresses a
    WHERE a._fivetran_deleted = FALSE
  )
  SELECT
    -- Create composite key from address components
    CONCAT_WS('|',
      COALESCE(sa.address_line, 'Unknown'),
      COALESCE(sa.address_line_2, ''),
      COALESCE(sa.city, 'Unknown'),
      COALESCE(sa.state, 'Unknown'),
      COALESCE(sa.zip, 'Unknown')
    ) AS address_key,
    sa.address_line,
    sa.address_line_2,
    sa.city,
    sa.state,
    sa.zip,
    sa.county,
    sa.residence_type,
    sa.years_at_home,
    sa.months_at_home,
    sa.time_zone,
    'bronze.leaseend_db_public.addresses' AS _source_table
  FROM source_addresses sa
  WHERE sa.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown|Unknown|Unknown|Unknown|Unknown' as address_key,
    'Unknown' as address_line,
    NULL as address_line_2,
    'Unknown' as city,
    'Unknown' as state,
    'Unknown' as zip,
    'Unknown' as county,
    'Unknown' as residence_type,
    NULL as years_at_home,
    NULL as months_at_home,
    'Unknown' as time_zone,
    'system' AS _source_table
) AS source
ON target.address_key = source.address_key

-- Update existing addresses if any attributes change
WHEN MATCHED AND (
    COALESCE(target.county, '') <> COALESCE(source.county, '') OR
    COALESCE(target.residence_type, '') <> COALESCE(source.residence_type, '') OR
    COALESCE(target.years_at_home, 0) <> COALESCE(source.years_at_home, 0) OR
    COALESCE(target.months_at_home, 0) <> COALESCE(source.months_at_home, 0) OR
    COALESCE(target.time_zone, '') <> COALESCE(source.time_zone, '')
  ) THEN
  UPDATE SET
    target.address_line = source.address_line,
    target.address_line_2 = source.address_line_2,
    target.city = source.city,
    target.state = source.state,
    target.zip = source.zip,
    target.county = source.county,
    target.residence_type = source.residence_type,
    target.years_at_home = source.years_at_home,
    target.months_at_home = source.months_at_home,
    target.time_zone = source.time_zone,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new addresses
WHEN NOT MATCHED THEN
  INSERT (
    address_key,
    address_line,
    address_line_2,
    city,
    state,
    zip,
    county,
    residence_type,
    years_at_home,
    months_at_home,
    time_zone,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.address_key,
    source.address_line,
    source.address_line_2,
    source.city,
    source.state,
    source.zip,
    source.county,
    source.residence_type,
    source.years_at_home,
    source.months_at_home,
    source.time_zone,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 