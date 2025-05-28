-- models/gold/dim/dim_processor.sql
-- Gold layer processor dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_processor (
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

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_processor AS target
USING (
  SELECT
    sp.processor_key,
    sp.processor_name,
    sp.processor_type,
    sp.is_internal,
    
    -- Determine processing speed based on type
    CASE
      WHEN sp.processor_type = 'VITU' THEN 'Fast (Same Day)'
      WHEN sp.processor_type = 'DMV' THEN 'Standard (3-5 days)'
      WHEN sp.processor_type = 'Third Party' THEN 'Variable (1-7 days)'
      WHEN sp.processor_type = 'Manual' THEN 'Slow (5-10 days)'
      ELSE 'Unknown'
    END AS processing_speed,
    
    CASE
      WHEN sp.processor_key = 'Unknown' THEN 'Unknown Processor'
      ELSE COALESCE(sp.processor_name, sp.processor_key, 'Unknown')
    END AS processor_display_name,
    
    sp._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_processor sp
) AS source
ON target.processor_key = source.processor_key

WHEN MATCHED THEN
  UPDATE SET
    target.processor_name = source.processor_name,
    target.processor_type = source.processor_type,
    target.is_internal = source.is_internal,
    target.processing_speed = source.processing_speed,
    target.processor_display_name = source.processor_display_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    processor_key,
    processor_name,
    processor_type,
    is_internal,
    processing_speed,
    processor_display_name,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.processor_key,
    source.processor_name,
    source.processor_type,
    source.is_internal,
    source.processing_speed,
    source.processor_display_name,
    source._source_table,
    source._load_timestamp
  ); 