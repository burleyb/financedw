-- models/gold/dim/dim_vsc_type.sql
-- Gold layer VSC type dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_vsc_type (
  vsc_type_key STRING NOT NULL,
  vsc_type_name STRING,
  coverage_level STRING,
  is_premium_coverage BOOLEAN,
  vsc_display_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer VSC type dimension with coverage analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_vsc_type AS target
USING (
  SELECT
    svt.vsc_type_key,
    svt.vsc_type_name,
    
    -- Determine coverage level based on type name
    CASE
      WHEN UPPER(svt.vsc_type_name) LIKE '%PREMIUM%' OR UPPER(svt.vsc_type_name) LIKE '%PLATINUM%' THEN 'Premium'
      WHEN UPPER(svt.vsc_type_name) LIKE '%GOLD%' OR UPPER(svt.vsc_type_name) LIKE '%ENHANCED%' THEN 'Enhanced'
      WHEN UPPER(svt.vsc_type_name) LIKE '%SILVER%' OR UPPER(svt.vsc_type_name) LIKE '%STANDARD%' THEN 'Standard'
      WHEN UPPER(svt.vsc_type_name) LIKE '%BASIC%' OR UPPER(svt.vsc_type_name) LIKE '%BRONZE%' THEN 'Basic'
      WHEN svt.vsc_type_name = 'Unknown' THEN 'Unknown'
      ELSE 'Standard'
    END AS coverage_level,
    
    -- Flag premium coverage
    CASE
      WHEN UPPER(svt.vsc_type_name) LIKE '%PREMIUM%' OR UPPER(svt.vsc_type_name) LIKE '%PLATINUM%' OR UPPER(svt.vsc_type_name) LIKE '%GOLD%' THEN TRUE
      ELSE FALSE
    END AS is_premium_coverage,
    
    CASE
      WHEN svt.vsc_type_key = 'Unknown' THEN 'No VSC Coverage'
      ELSE COALESCE(svt.vsc_type_name, svt.vsc_type_key, 'Unknown VSC')
    END AS vsc_display_name,
    
    svt._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_vsc_type svt
) AS source
ON target.vsc_type_key = source.vsc_type_key

WHEN MATCHED THEN
  UPDATE SET
    target.vsc_type_name = source.vsc_type_name,
    target.coverage_level = source.coverage_level,
    target.is_premium_coverage = source.is_premium_coverage,
    target.vsc_display_name = source.vsc_display_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    vsc_type_key,
    vsc_type_name,
    coverage_level,
    is_premium_coverage,
    vsc_display_name,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.vsc_type_key,
    source.vsc_type_name,
    source.coverage_level,
    source.is_premium_coverage,
    source.vsc_display_name,
    source._source_table,
    source._load_timestamp
  ); 