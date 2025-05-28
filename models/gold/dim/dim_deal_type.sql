-- models/gold/dim/dim_deal_type.sql
-- Gold layer deal type dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_deal_type (
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

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_deal_type AS target
USING (
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
  FROM silver.finance.dim_deal_type sdt
) AS source
ON target.deal_type_key = source.deal_type_key

WHEN MATCHED THEN
  UPDATE SET
    target.deal_type_description = source.deal_type_description,
    target.business_category = source.business_category,
    target.risk_profile = source.risk_profile,
    target.typical_loan_term = source.typical_loan_term,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    deal_type_key,
    deal_type_description,
    business_category,
    risk_profile,
    typical_loan_term,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_type_key,
    source.deal_type_description,
    source.business_category,
    source.risk_profile,
    source.typical_loan_term,
    source._source_table,
    source._load_timestamp
  ); 