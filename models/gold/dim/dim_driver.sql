-- models/gold/dim/dim_driver.sql
-- Gold layer driver dimension table reading from silver layer

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_driver;

CREATE TABLE gold.finance.dim_driver (
  driver_key STRING NOT NULL, -- Natural key from customers.id
  first_name STRING,
  middle_name STRING,
  last_name STRING,
  name_suffix STRING,
  full_name STRING, -- Computed full name
  email STRING,
  phone_number STRING,
  home_phone_number STRING,
  
  -- Address Information
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  
  -- Demographics
  date_of_birth DATE, -- From silver (dob)
  marital_status STRING,
  age INT,
  age_group STRING, -- Business enhancement
  
  -- Financial Information
  finscore DOUBLE,
  finscore_category STRING, -- Business enhancement
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer driver dimension with customer information and business enhancements'
PARTITIONED BY (state)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver layer with business enhancements
INSERT INTO gold.finance.dim_driver (
  driver_key,
  first_name,
  middle_name,
  last_name,
  name_suffix,
  full_name,
  email,
  phone_number,
  home_phone_number,
  city,
  state,
  zip,
  county,
  date_of_birth,
  marital_status,
  age,
  age_group,
  finscore,
  finscore_category,
  _source_table,
  _load_timestamp
)
SELECT
  dd.driver_key,
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
  dd.home_phone_number,
  
  -- Address Information
  dd.city,
  dd.state,
  dd.zip,
  dd.county,
  
  -- Demographics
  dd.dob as date_of_birth,
  dd.marital_status,
  dd.age,
  -- Age group categorization
  CASE
    WHEN dd.age IS NULL THEN 'Unknown'
    WHEN dd.age < 25 THEN 'Under 25'
    WHEN dd.age >= 25 AND dd.age < 35 THEN '25-34'
    WHEN dd.age >= 35 AND dd.age < 45 THEN '35-44'
    WHEN dd.age >= 45 AND dd.age < 55 THEN '45-54'
    WHEN dd.age >= 55 AND dd.age < 65 THEN '55-64'
    WHEN dd.age >= 65 THEN '65+'
    ELSE 'Unknown'
  END as age_group,
  
  -- Financial Information
  dd.finscore,
  -- Finscore categorization
  CASE
    WHEN dd.finscore IS NULL THEN 'Unknown'
    WHEN dd.finscore >= 800 THEN 'Excellent (800+)'
    WHEN dd.finscore >= 740 THEN 'Very Good (740-799)'
    WHEN dd.finscore >= 670 THEN 'Good (670-739)'
    WHEN dd.finscore >= 580 THEN 'Fair (580-669)'
    WHEN dd.finscore < 580 THEN 'Poor (<580)'
    ELSE 'Unknown'
  END as finscore_category,
  
  -- Metadata
  dd._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_driver dd
WHERE dd.driver_key IS NOT NULL; 