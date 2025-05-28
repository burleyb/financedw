-- models/dim/dim_vehicle.sql
-- Dimension table for vehicles, sourced from bronze.leaseend_db_public.cars

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_vehicle (
  vehicle_key STRING NOT NULL, -- Natural Key
  vin STRING NOT NULL, -- Natural Key
  deal_id STRING, -- Foreign Key
  make STRING,
  model STRING,
  model_year STRING,
  color STRING,
  vehicle_type STRING,
  fuel_type STRING,
  kbb_trim_name STRING,
  -- Add other relevant vehicle attributes if needed from the cars table
  _source_table STRING, -- Metadata: Originating source table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
PARTITIONED BY (make) -- Example partitioning
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.columnMapping.mode' = 'name' -- Required for IDENTITY columns
);

-- Add constraint if VIN should be unique (requires DBR 11.1+ or Unity Catalog)
-- ALTER TABLE gold.finance.dim_vehicle ADD CONSTRAINT dim_vehicle_vin_unique UNIQUE (vin);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_vehicle AS target
USING (
  -- Select the latest distinct vehicle data from the bronze cars table
  SELECT  
    LOWER(vin) AS vehicle_key, -- Natural Key
    COALESCE(CAST(LOWER(vin) AS STRING), 'Unknown') AS vin, -- Natural Key
    COALESCE(CAST(deal_id AS STRING), 'Unknown') AS deal_id,
    COALESCE(make, 'Unknown') AS make,
    COALESCE(model, 'Unknown') AS model,
    COALESCE(year, 'Unknown') AS model_year, -- Source column is 'year'
    COALESCE(color, 'Unknown') AS color,
    COALESCE(vehicle_type, 'Unknown') AS vehicle_type,
    COALESCE(fuel_type, 'Unknown') AS fuel_type,
    COALESCE(kbb_trim_name, 'Unknown') AS kbb_trim_name,
    'bronze.leaseend_db_public.cars' AS _source_table -- Static source table name
    -- Add other relevant attributes from bronze.leaseend_db_public.cars if needed
  FROM bronze.leaseend_db_public.cars
  WHERE vin IS NOT NULL -- Ensure we have a valid natural key
  -- Deduplicate based on VIN, taking the most recently updated record
  QUALIFY ROW_NUMBER() OVER (PARTITION BY vin ORDER BY updated_at DESC) = 1 -- Use updated_at from cars table
) AS source
ON target.vin = source.vin
AND target.deal_id = source.deal_id
-- Update existing vehicles if their data has changed (SCD Type 1)
WHEN MATCHED AND (
    target.make <> source.make OR
    target.model <> source.model OR
    target.model_year <> source.model_year OR
    target.color <> source.color OR
    target.vehicle_type <> source.vehicle_type OR
    target.fuel_type <> source.fuel_type OR
    -- Handle NULL comparisons carefully for kbb_trim_name
    (target.kbb_trim_name IS NULL AND source.kbb_trim_name IS NOT NULL) OR (target.kbb_trim_name IS NOT NULL AND source.kbb_trim_name IS NULL) OR (target.kbb_trim_name <> source.kbb_trim_name)
    -- Add checks for all other relevant attributes if added
  ) THEN
  UPDATE SET
    target.make = source.make,
    target.model = source.model,
    target.model_year = source.model_year,
    target.deal_id = source.deal_id,
    target.color = source.color,
    target.vehicle_type = source.vehicle_type,
    target.fuel_type = source.fuel_type,
    target.kbb_trim_name = source.kbb_trim_name,
    -- Update other attributes if added
    target._source_table = source._source_table, -- Update source table info
    target._load_timestamp = current_timestamp()

-- Insert new vehicles (vehicle_key is auto-generated)
WHEN NOT MATCHED THEN
  INSERT (
    vehicle_key,
    vin,
    deal_id,
    make,
    model,
    model_year,
    color,
    vehicle_type,
    fuel_type,
    kbb_trim_name,
    _source_table, -- Use _source_table
    _load_timestamp
  )
  VALUES (
    source.vehicle_key,
    source.vin,
    source.deal_id,
    source.make,
    source.model,
    source.model_year,
    source.color,
    source.vehicle_type,
    source.fuel_type,
    source.kbb_trim_name,
    source._source_table, -- Use _source_table
    current_timestamp()
  );


  -- Ensure 'Unknown' type exists for handling NULLs or empty strings
MERGE INTO gold.finance.dim_vehicle AS target
USING (SELECT 'Unknown' as vehicle_key, 'Unknown' as vin, 'Unknown' as deal_id, 'Unknown' as make, 'Unknown' as model, 'Unknown' as model_year, 'Unknown' as color, 'Unknown' as vehicle_type, 'Unknown' as fuel_type, 'Unknown' as kbb_trim_name, 'static' as _source_table) AS source
ON target.vehicle_key = source.vehicle_key
WHEN NOT MATCHED THEN INSERT (vehicle_key, vin, make, model, model_year, color, vehicle_type, fuel_type, kbb_trim_name, _source_table, _load_timestamp)
VALUES (source.vehicle_key, source.vin, source.make, source.model, source.model_year, source.color, source.vehicle_type, source.fuel_type, source.kbb_trim_name, source._source_table, current_timestamp());
