-- models/gold/dim/dim_source.sql
-- Gold layer source dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_source (
  source_key STRING NOT NULL,
  source_name STRING,
  source_category STRING,
  marketing_channel STRING,
  is_digital_channel BOOLEAN,
  is_paid_channel BOOLEAN,
  source_display_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer source dimension with marketing channel classifications'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_source AS target
USING (
  SELECT
    ss.source_key,
    ss.source_name,
    -- Categorize sources into business groups
    CASE
      WHEN UPPER(ss.source_name) LIKE '%GOOGLE%' OR UPPER(ss.source_name) LIKE '%FACEBOOK%' OR UPPER(ss.source_name) LIKE '%SOCIAL%' THEN 'Digital Advertising'
      WHEN UPPER(ss.source_name) LIKE '%REFERRAL%' OR UPPER(ss.source_name) LIKE '%WORD%' THEN 'Referral'
      WHEN UPPER(ss.source_name) LIKE '%DIRECT%' OR UPPER(ss.source_name) LIKE '%WEBSITE%' THEN 'Direct'
      WHEN UPPER(ss.source_name) LIKE '%EMAIL%' OR UPPER(ss.source_name) LIKE '%NEWSLETTER%' THEN 'Email Marketing'
      WHEN UPPER(ss.source_name) LIKE '%PHONE%' OR UPPER(ss.source_name) LIKE '%CALL%' THEN 'Phone'
      WHEN ss.source_name = 'Unknown' THEN 'Unknown'
      ELSE 'Other'
    END AS source_category,
    
    -- Map to marketing channels
    CASE
      WHEN UPPER(ss.source_name) LIKE '%GOOGLE%' THEN 'Google Ads'
      WHEN UPPER(ss.source_name) LIKE '%FACEBOOK%' THEN 'Facebook Ads'
      WHEN UPPER(ss.source_name) LIKE '%INSTAGRAM%' THEN 'Instagram'
      WHEN UPPER(ss.source_name) LIKE '%LINKEDIN%' THEN 'LinkedIn'
      WHEN UPPER(ss.source_name) LIKE '%TWITTER%' THEN 'Twitter'
      WHEN UPPER(ss.source_name) LIKE '%YOUTUBE%' THEN 'YouTube'
      WHEN UPPER(ss.source_name) LIKE '%EMAIL%' THEN 'Email'
      WHEN UPPER(ss.source_name) LIKE '%REFERRAL%' THEN 'Referral'
      WHEN UPPER(ss.source_name) LIKE '%DIRECT%' THEN 'Direct'
      WHEN UPPER(ss.source_name) LIKE '%ORGANIC%' THEN 'Organic Search'
      WHEN UPPER(ss.source_name) LIKE '%PHONE%' THEN 'Phone'
      ELSE 'Other'
    END AS marketing_channel,
    
    -- Flag digital channels
    CASE
      WHEN UPPER(ss.source_name) LIKE '%GOOGLE%' OR UPPER(ss.source_name) LIKE '%FACEBOOK%' OR 
           UPPER(ss.source_name) LIKE '%SOCIAL%' OR UPPER(ss.source_name) LIKE '%EMAIL%' OR
           UPPER(ss.source_name) LIKE '%WEBSITE%' OR UPPER(ss.source_name) LIKE '%ONLINE%' THEN TRUE
      ELSE FALSE
    END AS is_digital_channel,
    
    -- Flag paid channels
    CASE
      WHEN UPPER(ss.source_name) LIKE '%ADS%' OR UPPER(ss.source_name) LIKE '%PAID%' OR
           UPPER(ss.source_name) LIKE '%GOOGLE%' OR UPPER(ss.source_name) LIKE '%FACEBOOK%' THEN TRUE
      ELSE FALSE
    END AS is_paid_channel,
    
    -- Create display-friendly names
    CASE
      WHEN ss.source_key = 'Unknown' THEN 'Unknown Source'
      ELSE COALESCE(ss.source_name, ss.source_key, 'Unknown')
    END AS source_display_name,
    
    ss._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_source ss
) AS source
ON target.source_key = source.source_key

WHEN MATCHED THEN
  UPDATE SET
    target.source_name = source.source_name,
    target.source_category = source.source_category,
    target.marketing_channel = source.marketing_channel,
    target.is_digital_channel = source.is_digital_channel,
    target.is_paid_channel = source.is_paid_channel,
    target.source_display_name = source.source_display_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    source_key,
    source_name,
    source_category,
    marketing_channel,
    is_digital_channel,
    is_paid_channel,
    source_display_name,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.source_key,
    source.source_name,
    source.source_category,
    source.marketing_channel,
    source.is_digital_channel,
    source.is_paid_channel,
    source.source_display_name,
    source._source_table,
    source._load_timestamp
  ); 