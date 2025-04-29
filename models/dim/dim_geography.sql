-- models/dim/dim_geography.sql
-- Dimension table for geographic attributes

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_geography (
  geography_key STRING NOT NULL, -- Composite key based on state, county, zip, city
  city STRING,
  county STRING,
  state STRING,
  zip STRING,
  -- Add other relevant geographic attributes if needed (e.g., region, timezone)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct geographic combinations.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_geography AS target
USING (
  WITH source_geo AS (
      SELECT DISTINCT
          COALESCE(city, 'Unknown') as city_val,
          COALESCE(county, 'Unknown') as county_val,
          COALESCE(state, 'Unknown') as state_val,
          COALESCE(zip, 'Unknown') as zip_val
      FROM silver.deal.big_deal
      WHERE COALESCE(city, county, state, zip) IS NOT NULL -- Ensure at least one geo attribute exists
  )
  SELECT
    -- Create a composite key (adjust separator if needed)
    CONCAT(s.state_val, '|', s.county_val, '|', s.zip_val, '|', s.city_val) as geography_key,
    CASE WHEN s.city_val = 'Unknown' THEN NULL ELSE s.city_val END as city,
    CASE WHEN s.county_val = 'Unknown' THEN NULL ELSE s.county_val END as county,
    CASE WHEN s.state_val = 'Unknown' THEN NULL ELSE s.state_val END as state,
    CASE WHEN s.zip_val = 'Unknown' THEN NULL ELSE s.zip_val END as zip,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_geo s
) AS source
ON target.geography_key = source.geography_key

-- No UPDATE needed for SCD Type 1 if key components don't change

-- Insert new geographic combinations
WHEN NOT MATCHED THEN
  INSERT (geography_key, city, county, state, zip, _source_table, _load_timestamp)
  VALUES (source.geography_key, source.city, source.county, source.state, source.zip, source._source_table, CURRENT_TIMESTAMP());

-- Ensure 'Unknown' geography exists
MERGE INTO gold.finance.dim_geography AS target
USING (
    SELECT
        'Unknown|Unknown|Unknown|Unknown' as geography_key,
        NULL as city, NULL as county, 'Unknown' as state, NULL as zip, 'static' as _source_table
) AS source
ON target.geography_key = source.geography_key
WHEN NOT MATCHED THEN INSERT (geography_key, city, county, state, zip, _source_table, _load_timestamp)
VALUES (source.geography_key, source.city, source.county, source.state, source.zip, source._source_table, current_timestamp());

-- Note: This DDL only creates the table structure.
-- Populating it requires an ETL process that handles:
-- 1. Sourcing distinct combinations from bronze.leaseend_db_public.addresses, silver.deal.big_deal, etc.
-- 2. Deduplicating records.
-- 3. Implementing SCD Type 2 logic for effective/expiry dates and current flag.
-- 4. Handling NULLs and data cleansing (e.g., standardizing state abbreviations). 