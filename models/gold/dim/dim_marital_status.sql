-- models/gold/dim/dim_marital_status.sql
-- Gold layer marital status dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_marital_status (
  marital_status_key STRING NOT NULL,
  marital_status_description STRING,
  household_income_potential STRING,
  financial_stability_indicator STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer marital status dimension with financial stability analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_marital_status AS target
USING (
  SELECT
    sms.marital_status_key,
    sms.marital_status_description,
    
    -- Household income potential assessment
    CASE
      WHEN sms.marital_status_key = 'MARRIED' THEN 'Dual Income Potential'
      WHEN sms.marital_status_key = 'DOMESTIC_PARTNERSHIP' THEN 'Dual Income Potential'
      WHEN sms.marital_status_key = 'SINGLE' THEN 'Single Income'
      WHEN sms.marital_status_key = 'DIVORCED' THEN 'Single Income'
      WHEN sms.marital_status_key = 'WIDOWED' THEN 'Single Income'
      WHEN sms.marital_status_key = 'SEPARATED' THEN 'Variable Income'
      ELSE 'Unknown'
    END AS household_income_potential,
    
    -- Financial stability indicator
    CASE
      WHEN sms.marital_status_key = 'MARRIED' THEN 'High Stability'
      WHEN sms.marital_status_key = 'DOMESTIC_PARTNERSHIP' THEN 'High Stability'
      WHEN sms.marital_status_key = 'SINGLE' THEN 'Moderate Stability'
      WHEN sms.marital_status_key = 'WIDOWED' THEN 'Moderate Stability'
      WHEN sms.marital_status_key = 'DIVORCED' THEN 'Variable Stability'
      WHEN sms.marital_status_key = 'SEPARATED' THEN 'Low Stability'
      ELSE 'Unknown'
    END AS financial_stability_indicator,
    
    sms._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_marital_status sms
) AS source
ON target.marital_status_key = source.marital_status_key

WHEN MATCHED THEN
  UPDATE SET
    target.marital_status_description = source.marital_status_description,
    target.household_income_potential = source.household_income_potential,
    target.financial_stability_indicator = source.financial_stability_indicator,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    marital_status_key,
    marital_status_description,
    household_income_potential,
    financial_stability_indicator,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.marital_status_key,
    source.marital_status_description,
    source.household_income_potential,
    source.financial_stability_indicator,
    source._source_table,
    source._load_timestamp
  ); 