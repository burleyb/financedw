-- models/silver/dim/dim_source.sql
-- Silver layer source dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_source (
  source_key STRING NOT NULL, -- Natural key from source (e.g., source column)
  source_description STRING, -- Descriptive name for the source
  source_name_detail STRING, -- Detailed source name if available (from source_name)
  other_source_detail STRING, -- Additional description if available (from other_source_description)
  _source_table STRING, -- Metadata: Source table name
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Silver layer source dimension with deal source attributes'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_source AS target
USING (
  -- Source query: Select distinct source combinations from bronze deals
  WITH source_combinations AS (
      SELECT DISTINCT
          source,
          source_name,
          other_source_description
      FROM bronze.leaseend_db_public.deals
      WHERE source IS NOT NULL
        AND _fivetran_deleted = FALSE
  )
  SELECT
    s.source AS source_key,
    -- Derive description based on known source keys
    CASE s.source
        WHEN 'd2d' THEN 'Door-to-Door'
        WHEN 'outbound' THEN 'Outbound Call'
        WHEN 'call-in' THEN 'Inbound Call'
        WHEN 'web' THEN 'Web Lead'
        WHEN 'referral' THEN 'Referral'
        WHEN 'repeat' THEN 'Repeat Customer'
        WHEN 'social' THEN 'Social Media'
        WHEN 'email' THEN 'Email Campaign'
        WHEN 'sms' THEN 'SMS Campaign'
        ELSE COALESCE(s.source_name, s.source, 'Unknown') -- Use source_name if available, else source key
    END AS source_description,
    s.source_name AS source_name_detail,
    s.other_source_description AS other_source_detail,
    'bronze.leaseend_db_public.deals' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_combinations s
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'unknown' AS source_key,
    'Unknown' AS source_description,
    NULL AS source_name_detail,
    NULL AS other_source_detail,
    'system' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
) AS source
ON target.source_key = source.source_key

-- Update existing sources if description or details change
WHEN MATCHED AND (
    target.source_description <> source.source_description OR
    COALESCE(target.source_name_detail, '') <> COALESCE(source.source_name_detail, '') OR
    COALESCE(target.other_source_detail, '') <> COALESCE(source.other_source_detail, '')
  ) THEN
  UPDATE SET
    target.source_description = source.source_description,
    target.source_name_detail = source.source_name_detail,
    target.other_source_detail = source.other_source_detail,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new sources
WHEN NOT MATCHED THEN
  INSERT (
    source_key,
    source_description,
    source_name_detail,
    other_source_detail,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.source_key,
    source.source_description,
    source.source_name_detail,
    source.other_source_detail,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 