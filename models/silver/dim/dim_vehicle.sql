-- models/silver/dim/dim_vehicle.sql
-- Dimension table for vehicles, sourced from bronze.leaseend_db_public.cars

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS silver.finance.dim_vehicle (
  vehicle_key STRING NOT NULL, -- Natural Key (VIN)
  vin STRING NOT NULL, -- Natural Key
  deal_id STRING, -- Foreign Key
  make STRING,
  model STRING,
  model_year INT,
  color STRING,
  vehicle_type STRING,
  fuel_type STRING,
  kbb_trim_name STRING,
  kbb_valuation_date DATE,
  kbb_trade_in_value DOUBLE,
  kbb_retail_value DOUBLE,
  jdp_valuation_date DATE,
  jdp_trade_in_value DOUBLE,
  jdp_retail_value DOUBLE,
  mileage INT,
  odometer_status STRING,
  _source_table STRING, -- Metadata: Originating source table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
PARTITIONED BY (make) -- Partitioning by make for query optimization
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO silver.finance.dim_vehicle AS target
USING (
  -- Select the latest distinct vehicle data from the bronze cars table
  SELECT  
    UPPER(vin) AS vehicle_key, -- Natural Key
    COALESCE(CAST(UPPER(vin) AS STRING), 'Unknown') AS vin, -- Natural Key
    COALESCE(CAST(deal_id AS STRING), 'Unknown') AS deal_id,
    COALESCE(UPPER(make), 'Unknown') AS make,
    COALESCE(UPPER(model), 'Unknown') AS model,
    COALESCE(CAST(year AS INT), 0) AS model_year, -- Source column is 'year'
    COALESCE(color, 'Unknown') AS color,
    COALESCE(vehicle_type, 'Unknown') AS vehicle_type,
    COALESCE(fuel_type, 'Unknown') AS fuel_type,
    COALESCE(kbb_trim_name, 'Unknown') AS kbb_trim_name,
    CASE 
      WHEN kbb_valuation_date < '1900-01-01' OR kbb_valuation_date > '2037-12-31' 
      THEN NULL 
      ELSE kbb_valuation_date 
    END as kbb_valuation_date,
    kbb_trade_in_value,
    kbb_retail_value,
    CASE 
      WHEN jdp_valuation_date < '1900-01-01' OR jdp_valuation_date > '2037-12-31' 
      THEN NULL 
      ELSE jdp_valuation_date 
    END as jdp_valuation_date,
    jdp_trade_in_value,
    jdp_retail_value,
    mileage,
    odometer_status,
    'bronze.leaseend_db_public.cars' AS _source_table -- Static source table name
  FROM bronze.leaseend_db_public.cars
  WHERE vin IS NOT NULL -- Ensure we have a valid natural key
  -- Deduplicate based on VIN, taking the most recently updated record
  QUALIFY ROW_NUMBER() OVER (PARTITION BY vin ORDER BY updated_at DESC) = 1
) AS source
ON target.vin = source.vin

-- Update existing vehicles if their data has changed (SCD Type 1)
WHEN MATCHED AND (
    target.deal_id <> source.deal_id OR
    target.make <> source.make OR
    target.model <> source.model OR
    target.model_year <> source.model_year OR
    target.color <> source.color OR
    target.vehicle_type <> source.vehicle_type OR
    target.fuel_type <> source.fuel_type OR
    target.mileage <> source.mileage OR
    target.odometer_status <> source.odometer_status OR
    target.kbb_trade_in_value <> source.kbb_trade_in_value OR
    target.kbb_retail_value <> source.kbb_retail_value OR
    target.jdp_trade_in_value <> source.jdp_trade_in_value OR
    target.jdp_retail_value <> source.jdp_retail_value OR
    target.kbb_valuation_date <> source.kbb_valuation_date OR
    target.jdp_valuation_date <> source.jdp_valuation_date OR
    -- Handle NULL comparisons carefully for kbb_trim_name
    (target.kbb_trim_name IS NULL AND source.kbb_trim_name IS NOT NULL) OR 
    (target.kbb_trim_name IS NOT NULL AND source.kbb_trim_name IS NULL) OR 
    (target.kbb_trim_name <> source.kbb_trim_name)
  ) THEN
  UPDATE SET
    target.deal_id = source.deal_id,
    target.make = source.make,
    target.model = source.model,
    target.model_year = source.model_year,
    target.color = source.color,
    target.vehicle_type = source.vehicle_type,
    target.fuel_type = source.fuel_type,
    target.kbb_trim_name = source.kbb_trim_name,
    target.kbb_valuation_date = source.kbb_valuation_date,
    target.kbb_trade_in_value = source.kbb_trade_in_value,
    target.kbb_retail_value = source.kbb_retail_value,
    target.jdp_valuation_date = source.jdp_valuation_date,
    target.jdp_trade_in_value = source.jdp_trade_in_value,
    target.jdp_retail_value = source.jdp_retail_value,
    target.mileage = source.mileage,
    target.odometer_status = source.odometer_status,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new vehicles
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
    kbb_valuation_date,
    kbb_trade_in_value,
    kbb_retail_value,
    jdp_valuation_date,
    jdp_trade_in_value,
    jdp_retail_value,
    mileage,
    odometer_status,
    _source_table,
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
    source.kbb_valuation_date,
    source.kbb_trade_in_value,
    source.kbb_retail_value,
    source.jdp_valuation_date,
    source.jdp_trade_in_value,
    source.jdp_retail_value,
    source.mileage,
    source.odometer_status,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- Ensure 'Unknown' type exists for handling NULLs or empty strings
MERGE INTO silver.finance.dim_vehicle AS target
USING (
  SELECT 
    'Unknown' as vehicle_key, 
    'Unknown' as vin, 
    'Unknown' as deal_id, 
    'Unknown' as make, 
    'Unknown' as model, 
    0 as model_year, 
    'Unknown' as color, 
    'Unknown' as vehicle_type, 
    'Unknown' as fuel_type, 
    'Unknown' as kbb_trim_name,
    NULL as kbb_valuation_date,
    NULL as kbb_trade_in_value,
    NULL as kbb_retail_value,
    NULL as jdp_valuation_date,
    NULL as jdp_trade_in_value,
    NULL as jdp_retail_value,
    NULL as mileage,
    'Unknown' as odometer_status,
    'static' as _source_table
) AS source
ON target.vehicle_key = source.vehicle_key
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
    kbb_valuation_date,
    kbb_trade_in_value,
    kbb_retail_value,
    jdp_valuation_date,
    jdp_trade_in_value,
    jdp_retail_value,
    mileage,
    odometer_status,
    _source_table, 
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
    source.kbb_valuation_date,
    source.kbb_trade_in_value,
    source.kbb_retail_value,
    source.jdp_valuation_date,
    source.jdp_trade_in_value,
    source.jdp_retail_value,
    source.mileage,
    source.odometer_status,
    source._source_table, 
    CURRENT_TIMESTAMP()
  ); 