-- models/gold/dim/dim_geography.sql
-- Gold layer geography dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_geography (
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

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_geography AS target
USING (
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
    sg.time_zone,
    sg.state AS state_abbreviation,
    -- Identify major cities (simplified logic)
    CASE 
      WHEN UPPER(sg.city) IN ('NEW YORK', 'LOS ANGELES', 'CHICAGO', 'HOUSTON', 'PHOENIX', 'PHILADELPHIA', 
                              'SAN ANTONIO', 'SAN DIEGO', 'DALLAS', 'SAN JOSE', 'AUSTIN', 'JACKSONVILLE',
                              'FORT WORTH', 'COLUMBUS', 'CHARLOTTE', 'FRANCISCO', 'INDIANAPOLIS', 'SEATTLE',
                              'DENVER', 'WASHINGTON', 'BOSTON', 'EL PASO', 'DETROIT', 'NASHVILLE', 'PORTLAND',
                              'MEMPHIS', 'OKLAHOMA CITY', 'LAS VEGAS', 'LOUISVILLE', 'BALTIMORE', 'MILWAUKEE',
                              'ALBUQUERQUE', 'TUCSON', 'FRESNO', 'SACRAMENTO', 'MESA', 'KANSAS CITY', 'ATLANTA',
                              'LONG BEACH', 'COLORADO SPRINGS', 'RALEIGH', 'MIAMI', 'VIRGINIA BEACH', 'OMAHA',
                              'OAKLAND', 'MINNEAPOLIS', 'TULSA', 'ARLINGTON', 'TAMPA', 'NEW ORLEANS') THEN TRUE
      ELSE FALSE
    END AS is_major_city,
    -- Population category (simplified based on major city status)
    CASE 
      WHEN UPPER(sg.city) IN ('NEW YORK', 'LOS ANGELES', 'CHICAGO', 'HOUSTON', 'PHOENIX') THEN 'Large Metro (1M+)'
      WHEN UPPER(sg.city) IN ('PHILADELPHIA', 'SAN ANTONIO', 'SAN DIEGO', 'DALLAS', 'SAN JOSE', 'AUSTIN', 
                              'JACKSONVILLE', 'FORT WORTH', 'COLUMBUS', 'CHARLOTTE') THEN 'Major Metro (500K-1M)'
      WHEN sg.geography_key = 'Unknown' THEN 'Unknown'
      ELSE 'Small/Medium City (<500K)'
    END AS population_category,
    sg._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_geography sg
) AS source
ON target.geography_key = source.geography_key

WHEN MATCHED THEN
  UPDATE SET
    target.city = source.city,
    target.state = source.state,
    target.zip = source.zip,
    target.county = source.county,
    target.region = source.region,
    target.time_zone = source.time_zone,
    target.state_abbreviation = source.state_abbreviation,
    target.is_major_city = source.is_major_city,
    target.population_category = source.population_category,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
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
  VALUES (
    source.geography_key,
    source.city,
    source.state,
    source.zip,
    source.county,
    source.region,
    source.time_zone,
    source.state_abbreviation,
    source.is_major_city,
    source.population_category,
    source._source_table,
    source._load_timestamp
  ); 