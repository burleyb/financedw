-- models/gold/dim/dim_vsc_type.sql
-- Gold layer VSC type dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_vsc_type;

CREATE TABLE gold.finance.dim_vsc_type (
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

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_vsc_type (
  vsc_type_key,
  vsc_type_name,
  coverage_level,
  is_premium_coverage,
  vsc_display_name,
  _source_table,
  _load_timestamp
)
SELECT
  svt.vsc_type_key,
  svt.vsc_type_description AS vsc_type_name,  -- Silver has vsc_type_description, not vsc_type_name
  
  -- Determine coverage level based on type description
  CASE
    WHEN UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%PREMIUM%' OR 
         UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%PLATINUM%' THEN 'Premium'
    WHEN UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%GOLD%' OR 
         UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%ENHANCED%' THEN 'Enhanced'
    WHEN UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%SILVER%' OR 
         UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%STANDARD%' THEN 'Standard'
    WHEN UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%BASIC%' OR 
         UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%BRONZE%' THEN 'Basic'
    WHEN UPPER(COALESCE(svt.vsc_type_description, '')) = 'UNKNOWN' THEN 'Unknown'
    ELSE 'Standard'
  END AS coverage_level,
  
  -- Flag premium coverage
  CASE
    WHEN UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%PREMIUM%' OR 
         UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%PLATINUM%' OR 
         UPPER(COALESCE(svt.vsc_type_description, '')) LIKE '%GOLD%' THEN TRUE
    ELSE FALSE
  END AS is_premium_coverage,
  
  CASE
    WHEN UPPER(svt.vsc_type_key) = 'UNKNOWN' THEN 'No VSC Coverage'
    ELSE COALESCE(svt.vsc_type_description, svt.vsc_type_key, 'Unknown VSC')
  END AS vsc_display_name,
  
  svt._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_vsc_type svt; 