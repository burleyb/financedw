-- models/gold/dim/dim_pay_frequency.sql
-- Gold layer pay frequency dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_pay_frequency (
  pay_frequency_key STRING NOT NULL,
  pay_frequency_description STRING,
  annual_pay_periods INT,
  cash_flow_predictability STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer pay frequency dimension with cash flow analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_pay_frequency AS target
USING (
  SELECT
    spf.pay_frequency_key,
    spf.pay_frequency_description,
    
    -- Calculate annual pay periods
    CASE
      WHEN spf.pay_frequency_key = 'WEEKLY' THEN 52
      WHEN spf.pay_frequency_key = 'BIWEEKLY' THEN 26
      WHEN spf.pay_frequency_key = 'SEMIMONTHLY' THEN 24
      WHEN spf.pay_frequency_key = 'MONTHLY' THEN 12
      WHEN spf.pay_frequency_key = 'QUARTERLY' THEN 4
      WHEN spf.pay_frequency_key = 'ANNUALLY' THEN 1
      ELSE 0
    END AS annual_pay_periods,
    
    -- Cash flow predictability assessment
    CASE
      WHEN spf.pay_frequency_key IN ('WEEKLY', 'BIWEEKLY') THEN 'High Predictability'
      WHEN spf.pay_frequency_key IN ('SEMIMONTHLY', 'MONTHLY') THEN 'Good Predictability'
      WHEN spf.pay_frequency_key = 'QUARTERLY' THEN 'Moderate Predictability'
      WHEN spf.pay_frequency_key = 'ANNUALLY' THEN 'Low Predictability'
      ELSE 'Unknown'
    END AS cash_flow_predictability,
    
    spf._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_pay_frequency spf
) AS source
ON target.pay_frequency_key = source.pay_frequency_key

WHEN MATCHED THEN
  UPDATE SET
    target.pay_frequency_description = source.pay_frequency_description,
    target.annual_pay_periods = source.annual_pay_periods,
    target.cash_flow_predictability = source.cash_flow_predictability,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    pay_frequency_key,
    pay_frequency_description,
    annual_pay_periods,
    cash_flow_predictability,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.pay_frequency_key,
    source.pay_frequency_description,
    source.annual_pay_periods,
    source.cash_flow_predictability,
    source._source_table,
    source._load_timestamp
  ); 