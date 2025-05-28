-- models/gold/dim/dim_titling_pod.sql
-- Gold layer titling pod dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_titling_pod (
  titling_pod_key INT NOT NULL,
  titling_pod_name STRING,
  is_active BOOLEAN,
  team_size INT,
  specialization STRING,
  titling_pod_display_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer titling pod dimension with team specialization analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_titling_pod AS target
USING (
  SELECT
    stp.titling_pod_key,
    stp.titling_pod_name,
    stp.is_active,
    stp.team_size,
    
    -- Determine specialization based on name patterns
    CASE
      WHEN UPPER(stp.titling_pod_name) LIKE '%TITLE%' THEN 'Title Processing'
      WHEN UPPER(stp.titling_pod_name) LIKE '%REGISTRATION%' THEN 'Registration Processing'
      WHEN UPPER(stp.titling_pod_name) LIKE '%DMV%' THEN 'DMV Relations'
      WHEN UPPER(stp.titling_pod_name) LIKE '%EXPEDITE%' THEN 'Expedited Processing'
      ELSE 'General Processing'
    END AS specialization,
    
    CASE
      WHEN stp.titling_pod_key = -1 THEN 'Unknown Titling Pod'
      ELSE COALESCE(stp.titling_pod_name, CONCAT('Titling Pod ', stp.titling_pod_key))
    END AS titling_pod_display_name,
    
    stp._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_titling_pod stp
) AS source
ON target.titling_pod_key = source.titling_pod_key

WHEN MATCHED THEN
  UPDATE SET
    target.titling_pod_name = source.titling_pod_name,
    target.is_active = source.is_active,
    target.team_size = source.team_size,
    target.specialization = source.specialization,
    target.titling_pod_display_name = source.titling_pod_display_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    titling_pod_key,
    titling_pod_name,
    is_active,
    team_size,
    specialization,
    titling_pod_display_name,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.titling_pod_key,
    source.titling_pod_name,
    source.is_active,
    source.team_size,
    source.specialization,
    source.titling_pod_display_name,
    source._source_table,
    source._load_timestamp
  ); 