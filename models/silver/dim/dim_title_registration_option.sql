-- models/silver/dim/dim_title_registration_option.sql
-- Silver layer title registration option dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_title_registration_option (
  title_registration_option_key STRING NOT NULL, -- Natural key from source
  title_registration_option_description STRING, -- Descriptive name for the option
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer title registration option dimension storing distinct title and registration options'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_title_registration_option AS target
USING (
  WITH source_options AS (
    SELECT DISTINCT
      d.title_registration_option,
      ROW_NUMBER() OVER (PARTITION BY d.title_registration_option ORDER BY d.title_registration_option) as rn
    FROM bronze.leaseend_db_public.deals d
    WHERE d.title_registration_option IS NOT NULL 
      AND d.title_registration_option <> ''
      AND d._fivetran_deleted = FALSE
  )
  SELECT
    so.title_registration_option AS title_registration_option_key,
    -- Map title registration options to descriptive names
    CASE
      WHEN so.title_registration_option = 'title_and_registration_transfer' THEN 'Title & Registration Transfer'
      WHEN so.title_registration_option = 'title_only' THEN 'Title Only'
      WHEN so.title_registration_option = 'title_and_new_registration' THEN 'Title & New Registration'
      WHEN so.title_registration_option = 'registration_only' THEN 'Registration Only'
      WHEN so.title_registration_option = 'no_title_registration' THEN 'No Title or Registration'
      ELSE COALESCE(so.title_registration_option, 'Unknown')
    END AS title_registration_option_description,
    'bronze.leaseend_db_public.deals' AS _source_table
  FROM source_options so
  WHERE so.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as title_registration_option_key,
    'Unknown' as title_registration_option_description,
    'system' AS _source_table
) AS source
ON target.title_registration_option_key = source.title_registration_option_key

-- Update existing options if description changes
WHEN MATCHED AND (
    target.title_registration_option_description <> source.title_registration_option_description
  ) THEN
  UPDATE SET
    target.title_registration_option_description = source.title_registration_option_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new options
WHEN NOT MATCHED THEN
  INSERT (
    title_registration_option_key,
    title_registration_option_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.title_registration_option_key,
    source.title_registration_option_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 