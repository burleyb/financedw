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
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  _source_file_name STRING, -- Metadata: Originating file
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
    city,
    state,
    zip,
    county,
    _metadata.file_path AS _source_file_name -- Capture source file metadata
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
    (target.middle_name IS NULL AND source.middle_name IS NOT NULL) OR (target.middle_name IS NOT NULL AND source.middle_name IS NULL) OR (target.middle_name <> source.middle_name) OR
    (target.name_suffix IS NULL AND source.name_suffix IS NOT NULL) OR (target.name_suffix IS NOT NULL AND source.name_suffix IS NULL) OR (target.name_suffix <> source.name_suffix)
    -- Add checks for all other relevant attributes
  ) THEN
  UPDATE SET
    target.first_name = source.first_name,
    target.middle_name = source.middle_name,
    target.last_name = source.last_name,
    target.name_suffix = source.name_suffix,
    target.email = source.email,
    target.phone_number = source.phone_number,
    target.city = source.city,
    target.state = source.state,
    target.zip = source.zip,
    target.county = source.county,
    target._source_file_name = source._source_file_name,
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
    city,
    state,
    zip,
    county,
    _source_file_name,
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
    source.city,
    source.state,
    source.zip,
    source.county,
    source._source_file_name,
    CURRENT_TIMESTAMP()
  );