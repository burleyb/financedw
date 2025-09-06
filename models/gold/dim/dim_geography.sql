-- models/gold/dim/dim_geography.sql
-- Gold layer geography dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_geography;

CREATE TABLE gold.finance.dim_geography (
  geography_key STRING NOT NULL,
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  region STRING,
  time_zone STRING,
  state_abbreviation STRING,
  is_major_city BOOLEAN,
  population_category STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer geography dimension with regional classifications'
PARTITIONED BY (state)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all geographic data from silver with business enhancements
INSERT INTO gold.finance.dim_geography (
  geography_key,
  city,
  state,
  zip,
  county,
  region,
  time_zone,
  state_abbreviation,
  is_major_city,
  population_category,
  _source_table,
  _load_timestamp
)
SELECT
  sg.geography_key,
  sg.city,
  sg.state,
  sg.zip,
  sg.county,
  -- Add regional classifications
  CASE 
    WHEN sg.state IN ('CA', 'OR', 'WA', 'NV', 'AZ', 'UT', 'ID', 'MT', 'WY', 'CO', 'NM', 'AK', 'HI') THEN 'West'
    WHEN sg.state IN ('TX', 'OK', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY', 'WV', 'VA', 'NC', 'SC', 'GA', 'FL', 'MD', 'DE', 'DC') THEN 'South'
    WHEN sg.state IN ('ND', 'SD', 'NE', 'KS', 'MN', 'IA', 'MO', 'WI', 'IL', 'IN', 'MI', 'OH') THEN 'Midwest'
    WHEN sg.state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA') THEN 'Northeast'
    ELSE 'Unknown'
  END AS region,
  -- Generate time zone based on state (since silver doesn't have time_zone)
  CASE 
    WHEN sg.state IN ('CA', 'OR', 'WA', 'NV') THEN 'Pacific'
    WHEN sg.state IN ('AZ', 'UT', 'ID', 'MT', 'WY', 'CO', 'NM') THEN 'Mountain'
    WHEN sg.state IN ('TX', 'OK', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY', 'WI', 'IL', 'IN', 'MI', 'OH', 'ND', 'SD', 'NE', 'KS', 'MN', 'IA', 'MO') THEN 'Central'
    WHEN sg.state IN ('WV', 'VA', 'NC', 'SC', 'GA', 'FL', 'MD', 'DE', 'DC', 'ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA') THEN 'Eastern'
    WHEN sg.state IN ('AK') THEN 'Alaska'
    WHEN sg.state IN ('HI') THEN 'Hawaii'
    ELSE 'Unknown'
  END AS time_zone,
  sg.state AS state_abbreviation,
  -- Identify major cities (simplified logic)
  CASE 
    WHEN UPPER(COALESCE(sg.city, '')) IN ('NEW YORK', 'LOS ANGELES', 'CHICAGO', 'HOUSTON', 'PHOENIX', 'PHILADELPHIA', 
                          'SAN ANTONIO', 'SAN DIEGO', 'DALLAS', 'SAN JOSE', 'AUSTIN', 'JACKSONVILLE',
                          'FORT WORTH', 'COLUMBUS', 'CHARLOTTE', 'SAN FRANCISCO', 'INDIANAPOLIS', 'SEATTLE',
                          'DENVER', 'WASHINGTON', 'BOSTON', 'EL PASO', 'DETROIT', 'NASHVILLE', 'PORTLAND',
                          'MEMPHIS', 'OKLAHOMA CITY', 'LAS VEGAS', 'LOUISVILLE', 'BALTIMORE', 'MILWAUKEE',
                          'ALBUQUERQUE', 'TUCSON', 'FRESNO', 'SACRAMENTO', 'MESA', 'KANSAS CITY', 'ATLANTA',
                          'LONG BEACH', 'COLORADO SPRINGS', 'RALEIGH', 'MIAMI', 'VIRGINIA BEACH', 'OMAHA',
                          'OAKLAND', 'MINNEAPOLIS', 'TULSA', 'ARLINGTON', 'TAMPA', 'NEW ORLEANS') THEN TRUE
    ELSE FALSE
  END AS is_major_city,
  -- Population category (simplified based on major city status)
  CASE 
    WHEN UPPER(COALESCE(sg.city, '')) IN ('NEW YORK', 'LOS ANGELES', 'CHICAGO', 'HOUSTON', 'PHOENIX') THEN 'Large Metro (1M+)'
    WHEN UPPER(COALESCE(sg.city, '')) IN ('PHILADELPHIA', 'SAN ANTONIO', 'SAN DIEGO', 'DALLAS', 'SAN JOSE', 'AUSTIN', 
                          'JACKSONVILLE', 'FORT WORTH', 'COLUMBUS', 'CHARLOTTE') THEN 'Major Metro (500K-1M)'
    WHEN sg.geography_key = 'Unknown|Unknown|Unknown|Unknown' THEN 'Unknown'
    ELSE 'Small/Medium City (<500K)'
  END AS population_category,
  sg._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_geography sg; 