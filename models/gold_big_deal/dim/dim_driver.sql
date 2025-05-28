-- models/dim/dim_driver.sql
-- Dimension table for drivers

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_driver (
  driver_key STRING NOT NULL, -- Natural key (source system customer_id)
  first_name STRING,
  middle_name STRING,
  last_name STRING,
  name_suffix STRING,
  email STRING,
  phone_number STRING,
  home_phone_number STRING,
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  dob DATE,
  marital_status STRING,
  age INT,
  finscore DOUBLE,
  _source_table STRING, -- Metadata: Originating table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
PARTITIONED BY (state) -- Example partitioning, choose based on query patterns
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
    -- Removed 'delta.columnMapping.mode' = 'name' as IDENTITY column is removed
);

-- 2. Implement MERGE logic for incremental updates (SCD Type 1)
MERGE INTO gold.finance.dim_driver AS target
USING (
  -- Source query: Select latest distinct driver data from source
  SELECT
    customer_id AS driver_key, -- Use natural key directly
    first_name,
    middle_name,
    last_name,
    name_suffix,
    email,
    phone_number,
    home_phone_number,
    city,
    state,
    zip,
    county,
    CASE 
      WHEN dob < '1900-01-01' THEN NULL 
      ELSE dob 
    END as dob,
    marital_status,
    CASE 
      WHEN dob IS NOT NULL AND creation_date_utc IS NOT NULL 
        AND dob >= '1900-01-01'
      THEN CAST(FLOOR(MONTHS_BETWEEN(creation_date_utc, dob) / 12) AS INT)
      ELSE NULL 
    END as age,
    finscore,
    'silver.deal.big_deal' AS _source_table -- Capture source table metadata
  FROM silver.deal.big_deal
  WHERE customer_id IS NOT NULL -- Ensure we have a valid key
  -- Select the most recent record for each driver based on update_timestamp
  -- Assuming update_timestamp exists in silver.deal.big_deal, otherwise use a different timestamp like state_asof_utc
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY state_asof_utc DESC) = 1 -- Corrected timestamp column
) AS source
-- Match on the natural key
ON target.driver_key = source.driver_key

-- Update existing records if any attribute changed (simple overwrite)
WHEN MATCHED AND (
    target.first_name <> source.first_name OR
    target.last_name <> source.last_name OR
    target.email <> source.email OR
    target.phone_number <> source.phone_number OR
    target.city <> source.city OR
    target.state <> source.state OR
    target.zip <> source.zip OR
    target.county <> source.county OR
    target.dob <> source.dob OR
    target.marital_status <> source.marital_status OR
    target.age <> source.age OR
    target.finscore <> source.finscore OR
    (target.middle_name IS NULL AND source.middle_name IS NOT NULL) OR (target.middle_name IS NOT NULL AND source.middle_name IS NULL) OR (target.middle_name <> source.middle_name) OR
    (target.name_suffix IS NULL AND source.name_suffix IS NOT NULL) OR (target.name_suffix IS NOT NULL AND source.name_suffix IS NULL) OR (target.name_suffix <> source.name_suffix) OR
    (target.home_phone_number IS NULL AND source.home_phone_number IS NOT NULL) OR (target.home_phone_number IS NOT NULL AND source.home_phone_number IS NULL) OR (target.home_phone_number <> source.home_phone_number)
    -- Add checks for all other relevant attributes
  ) THEN
  UPDATE SET
    target.first_name = source.first_name,
    target.middle_name = source.middle_name,
    target.last_name = source.last_name,
    target.name_suffix = source.name_suffix,
    target.email = source.email,
    target.phone_number = source.phone_number,
    target.home_phone_number = source.home_phone_number,
    target.city = source.city,
    target.state = source.state,
    target.zip = source.zip,
    target.county = source.county,
    target.dob = source.dob,
    target.marital_status = source.marital_status,
    target.age = source.age,
    target.finscore = source.finscore,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new records (driver_key is the natural key)
WHEN NOT MATCHED THEN
  INSERT (
    driver_key,
    first_name,
    middle_name,
    last_name,
    name_suffix,
    email,
    phone_number,
    home_phone_number,
    city,
    state,
    zip,
    county,
    dob,
    marital_status,
    age,
    finscore,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.driver_key,
    source.first_name,
    source.middle_name,
    source.last_name,
    source.name_suffix,
    source.email,
    source.phone_number,
    source.home_phone_number,
    source.city,
    source.state,
    source.zip,
    source.county,
    source.dob,
    source.marital_status,
    source.age,
    source.finscore,
    source._source_table,
    CURRENT_TIMESTAMP()
  );