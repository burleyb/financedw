-- models/silver/dim/dim_geography.sql
-- Silver layer geography dimension table reading from bronze sources

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS silver.finance.dim_geography;

CREATE TABLE silver.finance.dim_geography (
  geography_key STRING NOT NULL, -- Composite key based on state, county, zip, city
  city STRING,
  county STRING,
  state STRING,
  zip STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer geography dimension with distinct geographic combinations'
PARTITIONED BY (state)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all geographic data from bronze sources
INSERT INTO silver.finance.dim_geography (
  geography_key,
  city,
  county,
  state,
  zip,
  _source_table,
  _load_timestamp
)
WITH source_geo AS (
  -- Get geographic data from addresses table only
  SELECT DISTINCT
      COALESCE(city, 'Unknown') as city_val,
      COALESCE(county, 'Unknown') as county_val,
      COALESCE(state, 'Unknown') as state_val,
      COALESCE(zip, 'Unknown') as zip_val
  FROM bronze.leaseend_db_public.addresses
  WHERE _fivetran_deleted = FALSE
    AND COALESCE(city, county, state, zip) IS NOT NULL -- Ensure at least one geo attribute exists
)
SELECT
  -- Create a composite key (adjust separator if needed)
  CONCAT(s.state_val, '|', s.county_val, '|', s.zip_val, '|', s.city_val) as geography_key,
  CASE WHEN s.city_val = 'Unknown' THEN NULL ELSE s.city_val END as city,
  CASE WHEN s.county_val = 'Unknown' THEN NULL ELSE s.county_val END as county,
  CASE WHEN s.state_val = 'Unknown' THEN NULL ELSE s.state_val END as state,
  CASE WHEN s.zip_val = 'Unknown' THEN NULL ELSE s.zip_val END as zip,
  'bronze.leaseend_db_public.addresses' as _source_table,
  CURRENT_TIMESTAMP() as _load_timestamp
FROM source_geo s

UNION ALL

-- Add standard unknown record
SELECT
  'Unknown|Unknown|Unknown|Unknown' as geography_key,
  NULL as city, 
  NULL as county, 
  'Unknown' as state, 
  NULL as zip, 
  'system' as _source_table,
  CURRENT_TIMESTAMP() as _load_timestamp; 