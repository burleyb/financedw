-- models/gold/dim/dim_odometer_status.sql
-- Gold layer odometer status dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_odometer_status (
  odometer_status_key STRING NOT NULL,
  odometer_status_description STRING,
  reliability_score INT,
  valuation_impact STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer odometer status dimension with valuation impact analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_odometer_status AS target
USING (
  SELECT
    sos.odometer_status_key,
    sos.odometer_status_description,
    
    -- Reliability scoring (1-10 scale)
    CASE
      WHEN sos.odometer_status_key = 'ACTUAL' THEN 10
      WHEN sos.odometer_status_key = 'NOT_ACTUAL' THEN 5
      WHEN sos.odometer_status_key = 'EXCEEDS_LIMIT' THEN 3
      WHEN sos.odometer_status_key = 'EXEMPT' THEN 7
      ELSE 1
    END AS reliability_score,
    
    -- Valuation impact assessment
    CASE
      WHEN sos.odometer_status_key = 'ACTUAL' THEN 'Positive Impact'
      WHEN sos.odometer_status_key = 'EXEMPT' THEN 'Neutral Impact'
      WHEN sos.odometer_status_key = 'NOT_ACTUAL' THEN 'Negative Impact'
      WHEN sos.odometer_status_key = 'EXCEEDS_LIMIT' THEN 'Significant Negative Impact'
      ELSE 'Unknown Impact'
    END AS valuation_impact,
    
    sos._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_odometer_status sos
) AS source
ON target.odometer_status_key = source.odometer_status_key

WHEN MATCHED THEN
  UPDATE SET
    target.odometer_status_description = source.odometer_status_description,
    target.reliability_score = source.reliability_score,
    target.valuation_impact = source.valuation_impact,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    odometer_status_key,
    odometer_status_description,
    reliability_score,
    valuation_impact,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.odometer_status_key,
    source.odometer_status_description,
    source.reliability_score,
    source.valuation_impact,
    source._source_table,
    source._load_timestamp
  ); 