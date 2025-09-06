-- models/gold/dim/dim_title_registration_option.sql
-- Gold layer title registration option dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_title_registration_option;

CREATE TABLE gold.finance.dim_title_registration_option (
  title_registration_option_key STRING NOT NULL,
  title_registration_option_description STRING,
  service_complexity STRING,
  processing_time_estimate STRING,
  fee_category STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer title registration option dimension with service complexity analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_title_registration_option (
  title_registration_option_key,
  title_registration_option_description,
  service_complexity,
  processing_time_estimate,
  fee_category,
  _source_table,
  _load_timestamp
)
SELECT
  stro.title_registration_option_key,
  stro.title_registration_option_description,
  
  -- Service complexity assessment
  CASE
    WHEN stro.title_registration_option_key = 'title_and_new_registration' THEN 'High Complexity'
    WHEN stro.title_registration_option_key = 'title_and_registration_transfer' THEN 'Medium Complexity'
    WHEN stro.title_registration_option_key = 'title_only' THEN 'Low Complexity'
    WHEN stro.title_registration_option_key = 'registration_only' THEN 'Low Complexity'
    WHEN stro.title_registration_option_key = 'no_title_registration' THEN 'No Service'
    ELSE 'Unknown'
  END AS service_complexity,
  
  -- Processing time estimates
  CASE
    WHEN stro.title_registration_option_key = 'title_and_new_registration' THEN '10-15 business days'
    WHEN stro.title_registration_option_key = 'title_and_registration_transfer' THEN '7-10 business days'
    WHEN stro.title_registration_option_key = 'title_only' THEN '5-7 business days'
    WHEN stro.title_registration_option_key = 'registration_only' THEN '3-5 business days'
    WHEN stro.title_registration_option_key = 'no_title_registration' THEN 'No Processing'
    ELSE 'Unknown'
  END AS processing_time_estimate,
  
  -- Fee categorization
  CASE
    WHEN stro.title_registration_option_key = 'title_and_new_registration' THEN 'Premium Fee'
    WHEN stro.title_registration_option_key = 'title_and_registration_transfer' THEN 'Standard Fee'
    WHEN stro.title_registration_option_key = 'title_only' THEN 'Basic Fee'
    WHEN stro.title_registration_option_key = 'registration_only' THEN 'Basic Fee'
    WHEN stro.title_registration_option_key = 'no_title_registration' THEN 'No Fee'
    ELSE 'Unknown'
  END AS fee_category,
  
  stro._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_title_registration_option stro; 