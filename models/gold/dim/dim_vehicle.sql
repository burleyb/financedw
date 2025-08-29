-- models/gold/dim/dim_vehicle.sql
-- Gold layer vehicle dimension table reading from silver layer

-- Drop existing table to fix schema mismatch and recreate with correct column names
DROP TABLE IF EXISTS gold.finance.dim_vehicle;

CREATE TABLE gold.finance.dim_vehicle (
  vehicle_key STRING NOT NULL, -- Natural key (VIN)
  vin STRING NOT NULL, -- Vehicle Identification Number
  deal_id STRING, -- Foreign Key
  make STRING,
  model STRING,
  model_year INT,
  color STRING,
  vehicle_type STRING,
  fuel_type STRING,
  kbb_trim_name STRING,
  kbb_valuation_date DATE,
  kbb_book_value DOUBLE,
  kbb_retail_book_value DOUBLE,
  jdp_valuation_date DATE,
  jdp_adjusted_clean_trade DOUBLE,
  jdp_adjusted_clean_retail DOUBLE,
  mileage INT,
  odometer_status STRING,
  _source_table STRING, -- Metadata: Originating source table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
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
    dv.deal_id,
    dv.make,
    dv.model,
    dv.model_year,
    dv.color,
    dv.vehicle_type,
    dv.fuel_type,
    dv.kbb_trim_name,
    dv.kbb_valuation_date,
    dv.kbb_book_value,
    dv.kbb_retail_book_value,
    dv.jdp_valuation_date,
    dv.jdp_adjusted_clean_trade,
    dv.jdp_adjusted_clean_retail,
    dv.mileage,
    dv.odometer_status,
    dv._source_table
    
  FROM silver.finance.dim_vehicle dv
  WHERE dv.vehicle_key IS NOT NULL
) AS source
ON target.vehicle_key = source.vehicle_key

-- Update existing vehicles if any attributes change (SCD Type 1)
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
    target.kbb_book_value <> source.kbb_book_value OR
    target.kbb_retail_book_value <> source.kbb_retail_book_value OR
    target.jdp_adjusted_clean_trade <> source.jdp_adjusted_clean_trade OR
    target.jdp_adjusted_clean_retail <> source.jdp_adjusted_clean_retail OR
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
    target.kbb_book_value = source.kbb_book_value,
    target.kbb_retail_book_value = source.kbb_retail_book_value,
    target.jdp_valuation_date = source.jdp_valuation_date,
    target.jdp_adjusted_clean_trade = source.jdp_adjusted_clean_trade,
    target.jdp_adjusted_clean_retail = source.jdp_adjusted_clean_retail,
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
    kbb_book_value,
    kbb_retail_book_value,
    jdp_valuation_date,
    jdp_adjusted_clean_trade,
    jdp_adjusted_clean_retail,
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
    source.kbb_book_value,
    source.kbb_retail_book_value,
    source.jdp_valuation_date,
    source.jdp_adjusted_clean_trade,
    source.jdp_adjusted_clean_retail,
    source.mileage,
    source.odometer_status,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 