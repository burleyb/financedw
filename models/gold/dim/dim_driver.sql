-- models/gold/dim/dim_driver.sql
-- Gold layer driver dimension table reading from silver layer

CREATE TABLE IF NOT EXISTS gold.finance.dim_driver (
  driver_key STRING NOT NULL, -- Natural key from customers.id
  customer_id_source STRING, -- Original customer ID for reference
  first_name STRING,
  middle_name STRING,
  last_name STRING,
  name_suffix STRING,
  full_name STRING, -- Computed full name
  email STRING,
  phone_number STRING,
  
  -- Current Address Information
  current_address_line1 STRING,
  current_address_line2 STRING,
  current_city STRING,
  current_state STRING,
  current_zip STRING,
  current_county STRING,
  
  -- Demographics
  age_at_first_deal INT,
  date_of_birth DATE, -- Masked/NULL for PII compliance
  
  -- Financial Information
  finscore DECIMAL(5,2),
  finscore_date DATE,
  
  -- Metadata
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer driver dimension with customer information and current address'
PARTITIONED BY (current_state)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge data from silver layer
MERGE INTO gold.finance.dim_driver AS target
USING (
  SELECT
    dd.driver_key,
    dd.customer_id_source,
    dd.first_name,
    dd.middle_name,
    dd.last_name,
    dd.name_suffix,
    -- Compute full name
    TRIM(CONCAT_WS(' ', 
      dd.first_name, 
      dd.middle_name, 
      dd.last_name, 
      dd.name_suffix
    )) as full_name,
    dd.email,
    dd.phone_number,
    
    -- Current Address
    dd.current_address_line1,
    dd.current_address_line2,
    dd.current_city,
    dd.current_state,
    dd.current_zip,
    dd.current_county,
    
    -- Demographics
    dd.age_at_first_deal,
    dd.date_of_birth, -- This should be NULL for PII compliance
    
    -- Financial Information
    dd.finscore,
    dd.finscore_date,
    
    -- Metadata
    dd.created_at,
    dd.updated_at,
    'silver.finance.dim_driver' as _source_table
    
  FROM silver.finance.dim_driver dd
  WHERE dd.driver_key IS NOT NULL
) AS source
ON target.driver_key = source.driver_key

-- Update existing drivers if any attributes change
WHEN MATCHED AND (
    COALESCE(target.first_name, '') <> COALESCE(source.first_name, '') OR
    COALESCE(target.last_name, '') <> COALESCE(source.last_name, '') OR
    COALESCE(target.email, '') <> COALESCE(source.email, '') OR
    COALESCE(target.phone_number, '') <> COALESCE(source.phone_number, '') OR
    COALESCE(target.current_address_line1, '') <> COALESCE(source.current_address_line1, '') OR
    COALESCE(target.current_city, '') <> COALESCE(source.current_city, '') OR
    COALESCE(target.current_state, '') <> COALESCE(source.current_state, '') OR
    COALESCE(target.current_zip, '') <> COALESCE(source.current_zip, '') OR
    COALESCE(target.finscore, 0) <> COALESCE(source.finscore, 0) OR
    COALESCE(target.updated_at, CAST('1900-01-01' AS TIMESTAMP)) <> COALESCE(source.updated_at, CAST('1900-01-01' AS TIMESTAMP))
  ) THEN
  UPDATE SET
    target.customer_id_source = source.customer_id_source,
    target.first_name = source.first_name,
    target.middle_name = source.middle_name,
    target.last_name = source.last_name,
    target.name_suffix = source.name_suffix,
    target.full_name = source.full_name,
    target.email = source.email,
    target.phone_number = source.phone_number,
    target.current_address_line1 = source.current_address_line1,
    target.current_address_line2 = source.current_address_line2,
    target.current_city = source.current_city,
    target.current_state = source.current_state,
    target.current_zip = source.current_zip,
    target.current_county = source.current_county,
    target.age_at_first_deal = source.age_at_first_deal,
    target.date_of_birth = source.date_of_birth,
    target.finscore = source.finscore,
    target.finscore_date = source.finscore_date,
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new drivers
WHEN NOT MATCHED THEN
  INSERT (
    driver_key, customer_id_source, first_name, middle_name, last_name, name_suffix, full_name,
    email, phone_number, current_address_line1, current_address_line2, current_city, current_state,
    current_zip, current_county, age_at_first_deal, date_of_birth, finscore, finscore_date,
    created_at, updated_at, _source_table, _load_timestamp
  )
  VALUES (
    source.driver_key, source.customer_id_source, source.first_name, source.middle_name, source.last_name,
    source.name_suffix, source.full_name, source.email, source.phone_number, source.current_address_line1,
    source.current_address_line2, source.current_city, source.current_state, source.current_zip,
    source.current_county, source.age_at_first_deal, source.date_of_birth, source.finscore, source.finscore_date,
    source.created_at, source.updated_at, source._source_table, CURRENT_TIMESTAMP()
  ); 