-- models/gold/dim/dim_vehicle.sql
-- Gold layer vehicle dimension table reading from silver layer

CREATE TABLE IF NOT EXISTS gold.finance.dim_vehicle (
  vehicle_key STRING NOT NULL, -- Natural key (VIN)
  vin STRING, -- Vehicle Identification Number
  make STRING,
  model STRING,
  model_year SMALLINT,
  color STRING,
  vehicle_type STRING,
  fuel_type STRING,
  
  -- Valuations
  kbb_trade_in_value DECIMAL(10,2),
  kbb_trade_in_date DATE,
  kbb_retail_value DECIMAL(10,2),
  kbb_retail_date DATE,
  kbb_trim_name STRING,
  
  jdp_trade_in_value DECIMAL(10,2),
  jdp_trade_in_date DATE,
  jdp_retail_value DECIMAL(10,2),
  jdp_retail_date DATE,
  
  -- Mileage Information
  mileage INT,
  odometer_status STRING,
  
  -- Metadata
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer vehicle dimension with VIN, specifications, and valuations'
PARTITIONED BY (make)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge data from silver layer
MERGE INTO gold.finance.dim_vehicle AS target
USING (
  SELECT
    dv.vehicle_key,
    dv.vin,
    dv.make,
    dv.model,
    dv.model_year,
    dv.color,
    dv.vehicle_type,
    dv.fuel_type,
    
    -- Valuations
    dv.kbb_trade_in_value,
    dv.kbb_trade_in_date,
    dv.kbb_retail_value,
    dv.kbb_retail_date,
    dv.kbb_trim_name,
    
    dv.jdp_trade_in_value,
    dv.jdp_trade_in_date,
    dv.jdp_retail_value,
    dv.jdp_retail_date,
    
    -- Mileage Information
    dv.mileage,
    dv.odometer_status,
    
    -- Metadata
    dv.created_at,
    dv.updated_at,
    'silver.finance.dim_vehicle' as _source_table
    
  FROM silver.finance.dim_vehicle dv
  WHERE dv.vehicle_key IS NOT NULL
) AS source
ON target.vehicle_key = source.vehicle_key

-- Update existing vehicles if any attributes change
WHEN MATCHED AND (
    COALESCE(target.make, '') <> COALESCE(source.make, '') OR
    COALESCE(target.model, '') <> COALESCE(source.model, '') OR
    COALESCE(target.model_year, 0) <> COALESCE(source.model_year, 0) OR
    COALESCE(target.color, '') <> COALESCE(source.color, '') OR
    COALESCE(target.kbb_trade_in_value, 0) <> COALESCE(source.kbb_trade_in_value, 0) OR
    COALESCE(target.kbb_retail_value, 0) <> COALESCE(source.kbb_retail_value, 0) OR
    COALESCE(target.jdp_trade_in_value, 0) <> COALESCE(source.jdp_trade_in_value, 0) OR
    COALESCE(target.jdp_retail_value, 0) <> COALESCE(source.jdp_retail_value, 0) OR
    COALESCE(target.mileage, 0) <> COALESCE(source.mileage, 0) OR
    COALESCE(target.updated_at, CAST('1900-01-01' AS TIMESTAMP)) <> COALESCE(source.updated_at, CAST('1900-01-01' AS TIMESTAMP))
  ) THEN
  UPDATE SET
    target.vin = source.vin,
    target.make = source.make,
    target.model = source.model,
    target.model_year = source.model_year,
    target.color = source.color,
    target.vehicle_type = source.vehicle_type,
    target.fuel_type = source.fuel_type,
    target.kbb_trade_in_value = source.kbb_trade_in_value,
    target.kbb_trade_in_date = source.kbb_trade_in_date,
    target.kbb_retail_value = source.kbb_retail_value,
    target.kbb_retail_date = source.kbb_retail_date,
    target.kbb_trim_name = source.kbb_trim_name,
    target.jdp_trade_in_value = source.jdp_trade_in_value,
    target.jdp_trade_in_date = source.jdp_trade_in_date,
    target.jdp_retail_value = source.jdp_retail_value,
    target.jdp_retail_date = source.jdp_retail_date,
    target.mileage = source.mileage,
    target.odometer_status = source.odometer_status,
    target.created_at = source.created_at,
    target.updated_at = source.updated_at,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new vehicles
WHEN NOT MATCHED THEN
  INSERT (
    vehicle_key, vin, make, model, model_year, color, vehicle_type, fuel_type,
    kbb_trade_in_value, kbb_trade_in_date, kbb_retail_value, kbb_retail_date, kbb_trim_name,
    jdp_trade_in_value, jdp_trade_in_date, jdp_retail_value, jdp_retail_date,
    mileage, odometer_status, created_at, updated_at, _source_table, _load_timestamp
  )
  VALUES (
    source.vehicle_key, source.vin, source.make, source.model, source.model_year, source.color,
    source.vehicle_type, source.fuel_type, source.kbb_trade_in_value, source.kbb_trade_in_date,
    source.kbb_retail_value, source.kbb_retail_date, source.kbb_trim_name, source.jdp_trade_in_value,
    source.jdp_trade_in_date, source.jdp_retail_value, source.jdp_retail_date, source.mileage,
    source.odometer_status, source.created_at, source.updated_at, source._source_table, CURRENT_TIMESTAMP()
  ); 