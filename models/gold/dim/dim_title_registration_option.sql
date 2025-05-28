-- models/gold/dim/dim_title_registration_option.sql
-- Gold layer title registration option dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_title_registration_option (
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

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_title_registration_option AS target
USING (
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
  FROM silver.finance.dim_title_registration_option stro
) AS source
ON target.title_registration_option_key = source.title_registration_option_key

WHEN MATCHED THEN
  UPDATE SET
    target.title_registration_option_description = source.title_registration_option_description,
    target.service_complexity = source.service_complexity,
    target.processing_time_estimate = source.processing_time_estimate,
    target.fee_category = source.fee_category,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    title_registration_option_key,
    title_registration_option_description,
    service_complexity,
    processing_time_estimate,
    fee_category,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.title_registration_option_key,
    source.title_registration_option_description,
    source.service_complexity,
    source.processing_time_estimate,
    source.fee_category,
    source._source_table,
    source._load_timestamp
  ); 