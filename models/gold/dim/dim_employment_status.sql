-- models/gold/dim/dim_employment_status.sql
-- Gold layer employment status dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_employment_status (
  employment_status_key STRING NOT NULL,
  employment_status_description STRING,
  income_stability_score INT,
  risk_category STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer employment status dimension with risk assessment'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_employment_status AS target
USING (
  SELECT
    ses.employment_status_key,
    ses.employment_status_description,
    
    -- Income stability scoring (1-10 scale)
    CASE
      WHEN ses.employment_status_key = 'EMPLOYED' THEN 8
      WHEN ses.employment_status_key = 'SELF_EMPLOYED' THEN 6
      WHEN ses.employment_status_key = 'RETIRED' THEN 7
      WHEN ses.employment_status_key = 'STUDENT' THEN 4
      WHEN ses.employment_status_key = 'UNEMPLOYED' THEN 2
      WHEN ses.employment_status_key = 'DISABLED' THEN 5
      ELSE 1
    END AS income_stability_score,
    
    -- Risk categorization for lending
    CASE
      WHEN ses.employment_status_key = 'EMPLOYED' THEN 'Low Risk'
      WHEN ses.employment_status_key = 'RETIRED' THEN 'Low Risk'
      WHEN ses.employment_status_key = 'SELF_EMPLOYED' THEN 'Medium Risk'
      WHEN ses.employment_status_key = 'DISABLED' THEN 'Medium Risk'
      WHEN ses.employment_status_key = 'STUDENT' THEN 'High Risk'
      WHEN ses.employment_status_key = 'UNEMPLOYED' THEN 'High Risk'
      ELSE 'Unknown Risk'
    END AS risk_category,
    
    ses._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_employment_status ses
) AS source
ON target.employment_status_key = source.employment_status_key

WHEN MATCHED THEN
  UPDATE SET
    target.employment_status_description = source.employment_status_description,
    target.income_stability_score = source.income_stability_score,
    target.risk_category = source.risk_category,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    employment_status_key,
    employment_status_description,
    income_stability_score,
    risk_category,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.employment_status_key,
    source.employment_status_description,
    source.income_stability_score,
    source.risk_category,
    source._source_table,
    source._load_timestamp
  ); 