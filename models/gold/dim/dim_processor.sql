-- models/gold/dim/dim_processor.sql
-- Gold layer processor dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_processor;

CREATE TABLE gold.finance.dim_processor (
  processor_key STRING NOT NULL,
  processor_name STRING,
  processor_type STRING,
  is_internal BOOLEAN,
  processing_speed STRING,
  processor_display_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer processor dimension with processing capability analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_processor (
  processor_key,
  processor_name,
  processor_type,
  is_internal,
  processing_speed,
  processor_display_name,
  _source_table,
  _load_timestamp
)
SELECT
  sp.processor_key,
  sp.processor_description AS processor_name,  -- Silver has processor_description, not processor_name
  sp.processor_type,
  
  -- Determine if processor is internal based on processor key patterns
  CASE
    WHEN UPPER(sp.processor_key) IN ('INTERNAL', 'MANUAL') THEN TRUE
    WHEN UPPER(sp.processor_key) IN ('VITU', 'DLRDMV', 'THE TITLE GIRL', 'ATC', 'WKLS', 'PLATEMAN', 'DLR50') THEN FALSE
    WHEN UPPER(sp.processor_key) = 'STATE DMV' THEN FALSE
    ELSE FALSE  -- Default to external
  END AS is_internal,
  
  -- Determine processing speed based on processor key patterns
  CASE
    WHEN UPPER(sp.processor_key) = 'VITU' THEN 'Fast (Same Day)'
    WHEN UPPER(sp.processor_key) IN ('STATE DMV', 'DLRDMV') THEN 'Standard (3-5 days)'
    WHEN UPPER(sp.processor_key) IN ('THE TITLE GIRL', 'ATC', 'WKLS', 'PLATEMAN', 'DLR50') THEN 'Variable (1-7 days)'
    WHEN UPPER(sp.processor_key) = 'MANUAL' THEN 'Slow (5-10 days)'
    WHEN UPPER(sp.processor_key) = 'INTERNAL' THEN 'Fast (Same Day)'
    ELSE 'Unknown'
  END AS processing_speed,
  
  CASE
    WHEN sp.processor_key = 'UNKNOWN' THEN 'Unknown Processor'
    ELSE COALESCE(sp.processor_description, sp.processor_key, 'Unknown')
  END AS processor_display_name,
  
  sp._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_processor sp; 