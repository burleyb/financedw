-- models/silver/dim/dim_source.sql
-- Silver layer source dimension table reading from bronze sources

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS silver.finance.dim_source;

CREATE TABLE silver.finance.dim_source (
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

-- Insert all source data from bronze tables
INSERT INTO silver.finance.dim_source (
  source_key,
  source_description,
  source_name_detail,
  other_source_detail,
  _source_table,
  _load_timestamp
)
WITH source_combinations AS (
  -- Get distinct source values from deals table
  SELECT DISTINCT
    d.source,
    NULL as source_name,
    NULL as other_source_description
  FROM bronze.leaseend_db_public.deals d
  WHERE d.source IS NOT NULL
    AND d._fivetran_deleted = FALSE
  
  UNION
  
  -- Get distinct source combinations from referrals table
  SELECT DISTINCT
    d.source,
    r.source_name,
    r.other_source_description
  FROM bronze.leaseend_db_public.deals d
  INNER JOIN bronze.leaseend_db_public.deals_referrals_sources r 
    ON d.id = r.deal_id
  WHERE d.source IS NOT NULL
    AND d._fivetran_deleted = FALSE
    AND r._fivetran_deleted = FALSE
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
  'bronze.leaseend_db_public.deals+deals_referrals_sources' as _source_table,
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
  CURRENT_TIMESTAMP() as _load_timestamp; 