-- models/gold/dim/dim_source.sql
-- Gold layer source dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_source;

CREATE TABLE gold.finance.dim_source (
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

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_source (
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
SELECT
  ss.source_key,
  ss.source_description AS source_name,  -- Silver has source_description, not source_name
  
  -- Categorize sources into business groups based on source_key and description
  CASE
    WHEN ss.source_key = 'web' THEN 'Digital'
    WHEN ss.source_key = 'social' THEN 'Digital Advertising'
    WHEN ss.source_key = 'email' THEN 'Email Marketing'
    WHEN ss.source_key = 'sms' THEN 'SMS Marketing'
    WHEN ss.source_key = 'referral' THEN 'Referral'
    WHEN ss.source_key = 'repeat' THEN 'Repeat Customer'
    WHEN ss.source_key IN ('call-in', 'outbound') THEN 'Phone'
    WHEN ss.source_key = 'd2d' THEN 'Door-to-Door'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%GOOGLE%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%FACEBOOK%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%SOCIAL%' THEN 'Digital Advertising'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%REFERRAL%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%WORD%' THEN 'Referral'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%EMAIL%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%NEWSLETTER%' THEN 'Email Marketing'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%PHONE%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%CALL%' THEN 'Phone'
    WHEN ss.source_key = 'unknown' THEN 'Unknown'
    ELSE 'Other'
  END AS source_category,
  
  -- Map to marketing channels
  CASE
    WHEN ss.source_key = 'web' THEN 'Website'
    WHEN ss.source_key = 'social' THEN 'Social Media'
    WHEN ss.source_key = 'email' THEN 'Email'
    WHEN ss.source_key = 'sms' THEN 'SMS'
    WHEN ss.source_key = 'referral' THEN 'Referral'
    WHEN ss.source_key = 'repeat' THEN 'Repeat Customer'
    WHEN ss.source_key = 'call-in' THEN 'Inbound Phone'
    WHEN ss.source_key = 'outbound' THEN 'Outbound Phone'
    WHEN ss.source_key = 'd2d' THEN 'Door-to-Door'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%GOOGLE%' THEN 'Google Ads'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%FACEBOOK%' THEN 'Facebook Ads'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%INSTAGRAM%' THEN 'Instagram'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%LINKEDIN%' THEN 'LinkedIn'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%TWITTER%' THEN 'Twitter'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%YOUTUBE%' THEN 'YouTube'
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%ORGANIC%' THEN 'Organic Search'
    ELSE 'Other'
  END AS marketing_channel,
  
  -- Flag digital channels
  CASE
    WHEN ss.source_key IN ('web', 'social', 'email', 'sms') THEN TRUE
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%GOOGLE%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%FACEBOOK%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%SOCIAL%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%EMAIL%' OR
         UPPER(COALESCE(ss.source_description, '')) LIKE '%WEBSITE%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%ONLINE%' THEN TRUE
    ELSE FALSE
  END AS is_digital_channel,
  
  -- Flag paid channels
  CASE
    WHEN UPPER(COALESCE(ss.source_description, '')) LIKE '%ADS%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%PAID%' OR
         UPPER(COALESCE(ss.source_description, '')) LIKE '%GOOGLE%' OR 
         UPPER(COALESCE(ss.source_description, '')) LIKE '%FACEBOOK%' THEN TRUE
    ELSE FALSE
  END AS is_paid_channel,
  
  -- Create display-friendly names
  CASE
    WHEN ss.source_key = 'unknown' THEN 'Unknown Source'
    ELSE COALESCE(ss.source_description, ss.source_key, 'Unknown')
  END AS source_display_name,
  
  ss._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_source ss; 