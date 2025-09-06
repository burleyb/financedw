-- models/gold/dim/dim_deal_type.sql
-- Gold layer deal type dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_deal_type;

CREATE TABLE gold.finance.dim_deal_type (
  deal_type_key STRING NOT NULL,
  deal_type_description STRING,
  business_category STRING,
  risk_profile STRING,
  typical_loan_term STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer deal type dimension with business risk and term analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_deal_type (
  deal_type_key,
  deal_type_description,
  business_category,
  risk_profile,
  typical_loan_term,
  _source_table,
  _load_timestamp
)
SELECT
  sdt.deal_type_key,
  sdt.deal_type_description,
  
  -- Business categorization
  CASE
    WHEN sdt.deal_type_key = 'acquisition' THEN 'New Business'
    WHEN sdt.deal_type_key = 'refi' THEN 'Refinancing'
    WHEN sdt.deal_type_key = 'buyout' THEN 'Lease Conversion'
    WHEN sdt.deal_type_key = 'purchase' THEN 'Direct Purchase'
    WHEN sdt.deal_type_key = 'lease_end' THEN 'Lease Termination'
    ELSE 'Other'
  END AS business_category,
  
  -- Risk profile assessment
  CASE
    WHEN sdt.deal_type_key = 'refi' THEN 'Low Risk'
    WHEN sdt.deal_type_key = 'buyout' THEN 'Low Risk'
    WHEN sdt.deal_type_key = 'acquisition' THEN 'Medium Risk'
    WHEN sdt.deal_type_key = 'purchase' THEN 'Medium Risk'
    WHEN sdt.deal_type_key = 'lease_end' THEN 'Variable Risk'
    ELSE 'Unknown Risk'
  END AS risk_profile,
  
  -- Typical loan terms
  CASE
    WHEN sdt.deal_type_key = 'refi' THEN '36-72 months'
    WHEN sdt.deal_type_key = 'buyout' THEN '24-60 months'
    WHEN sdt.deal_type_key = 'acquisition' THEN '48-84 months'
    WHEN sdt.deal_type_key = 'purchase' THEN '36-72 months'
    WHEN sdt.deal_type_key = 'lease_end' THEN 'Variable'
    ELSE 'Unknown'
  END AS typical_loan_term,
  
  sdt._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_deal_type sdt; 