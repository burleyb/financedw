-- models/gold/dim/dim_address.sql
-- Gold layer address dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_address (
  address_key STRING NOT NULL,
  address_line STRING,
  address_line_2 STRING,
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  residence_type STRING,
  years_at_home INT,
  months_at_home INT,
  time_zone STRING,
  total_months_at_home INT,
  residence_stability STRING,
  housing_category STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer address dimension with residence stability analysis'
PARTITIONED BY (state)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_address AS target
USING (
  SELECT
    sa.address_key,
    sa.address_line,
    sa.address_line_2,
    sa.city,
    sa.state,
    sa.zip,
    sa.county,
    sa.residence_type,
    sa.years_at_home,
    sa.months_at_home,
    sa.time_zone,
    
    -- Calculate total months for easier analysis
    COALESCE(sa.years_at_home, 0) * 12 + COALESCE(sa.months_at_home, 0) AS total_months_at_home,
    
    -- Residence stability assessment
    CASE
      WHEN COALESCE(sa.years_at_home, 0) >= 5 THEN 'Very Stable (5+ years)'
      WHEN COALESCE(sa.years_at_home, 0) >= 2 THEN 'Stable (2-5 years)'
      WHEN COALESCE(sa.years_at_home, 0) >= 1 THEN 'Moderate (1-2 years)'
      WHEN COALESCE(sa.years_at_home, 0) * 12 + COALESCE(sa.months_at_home, 0) >= 6 THEN 'New (6-12 months)'
      WHEN COALESCE(sa.years_at_home, 0) * 12 + COALESCE(sa.months_at_home, 0) > 0 THEN 'Very New (<6 months)'
      ELSE 'Unknown'
    END AS residence_stability,
    
    -- Housing category
    CASE
      WHEN UPPER(sa.residence_type) LIKE '%OWN%' THEN 'Homeowner'
      WHEN UPPER(sa.residence_type) LIKE '%RENT%' THEN 'Renter'
      WHEN UPPER(sa.residence_type) LIKE '%FAMILY%' OR UPPER(sa.residence_type) LIKE '%RELATIVE%' THEN 'Family Housing'
      WHEN sa.residence_type = 'Unknown' THEN 'Unknown'
      ELSE 'Other'
    END AS housing_category,
    
    sa._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_address sa
) AS source
ON target.address_key = source.address_key

WHEN MATCHED THEN
  UPDATE SET
    target.address_line = source.address_line,
    target.address_line_2 = source.address_line_2,
    target.city = source.city,
    target.state = source.state,
    target.zip = source.zip,
    target.county = source.county,
    target.residence_type = source.residence_type,
    target.years_at_home = source.years_at_home,
    target.months_at_home = source.months_at_home,
    target.time_zone = source.time_zone,
    target.total_months_at_home = source.total_months_at_home,
    target.residence_stability = source.residence_stability,
    target.housing_category = source.housing_category,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    address_key,
    address_line,
    address_line_2,
    city,
    state,
    zip,
    county,
    residence_type,
    years_at_home,
    months_at_home,
    time_zone,
    total_months_at_home,
    residence_stability,
    housing_category,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.address_key,
    source.address_line,
    source.address_line_2,
    source.city,
    source.state,
    source.zip,
    source.county,
    source.residence_type,
    source.years_at_home,
    source.months_at_home,
    source.time_zone,
    source.total_months_at_home,
    source.residence_stability,
    source.housing_category,
    source._source_table,
    source._load_timestamp
  ); 