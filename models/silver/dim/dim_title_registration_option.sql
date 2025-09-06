-- models/silver/dim/dim_title_registration_option.sql
-- Silver layer title registration option dimension table reading from bronze sources

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS silver.finance.dim_title_registration_option;

CREATE TABLE silver.finance.dim_title_registration_option (
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

-- Insert title registration options from bronze.leaseend_db_public.financial_infos
INSERT INTO silver.finance.dim_title_registration_option (
  title_registration_option_key,
  title_registration_option_description,
  _source_table,
  _load_timestamp
)
WITH source_options AS (
  SELECT DISTINCT
    fi.title_registration_option,
    ROW_NUMBER() OVER (PARTITION BY fi.title_registration_option ORDER BY fi.title_registration_option) as rn
  FROM bronze.leaseend_db_public.financial_infos fi
  WHERE fi.title_registration_option IS NOT NULL 
    AND fi.title_registration_option <> ''
    AND (fi._fivetran_deleted = FALSE OR fi._fivetran_deleted IS NULL)
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
  'bronze.leaseend_db_public.financial_infos' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM source_options so
WHERE so.rn = 1

UNION ALL

-- Add standard unknown record
SELECT
  'Unknown' as title_registration_option_key,
  'Unknown' as title_registration_option_description,
  'system' AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp; 