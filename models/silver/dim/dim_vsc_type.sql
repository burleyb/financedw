-- models/silver/dim/dim_vsc_type.sql
-- Silver layer VSC type dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_vsc_type (
  vsc_type_key STRING NOT NULL, -- Natural key from source (e.g., vsc_type)
  vsc_type_description STRING, -- Descriptive name for the VSC type
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Silver layer VSC type dimension with distinct VSC product types'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_vsc_type AS target
USING (
  -- Source query: Select distinct VSC types from bronze financial_infos
  WITH source_types AS (
      SELECT DISTINCT vsc_type
      FROM bronze.leaseend_db_public.financial_infos
      WHERE vsc_type IS NOT NULL 
        AND vsc_type <> '' 
        AND _fivetran_deleted = FALSE
  )
  SELECT
    s.vsc_type AS vsc_type_key,
    -- Map VSC types to descriptive names
    CASE
      WHEN UPPER(s.vsc_type) = 'VALUE' THEN 'Value Coverage'
      WHEN UPPER(s.vsc_type) = 'BASIC' THEN 'Basic Coverage'
      WHEN UPPER(s.vsc_type) = 'MAJOR' THEN 'Major Coverage'
      WHEN UPPER(s.vsc_type) = 'PREMIUM' THEN 'Premium Coverage'
      WHEN UPPER(s.vsc_type) = 'EXTENDED' THEN 'Extended Coverage'
      ELSE COALESCE(s.vsc_type, 'Unknown') -- Handle potential future options
    END AS vsc_type_description,
    'bronze.leaseend_db_public.financial_infos' as _source_table
  FROM source_types s
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as vsc_type_key,
    'Unknown' as vsc_type_description,
    'system' as _source_table
) AS source
ON target.vsc_type_key = source.vsc_type_key

-- Update existing types if description changes
WHEN MATCHED AND (
    target.vsc_type_description <> source.vsc_type_description
  ) THEN
  UPDATE SET
    target.vsc_type_description = source.vsc_type_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new types
WHEN NOT MATCHED THEN
  INSERT (
    vsc_type_key,
    vsc_type_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.vsc_type_key,
    source.vsc_type_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 