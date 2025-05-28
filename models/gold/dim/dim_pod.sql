-- models/gold/dim/dim_pod.sql
-- Gold layer pod dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_pod (
  pod_key INT NOT NULL,
  pod_name STRING,
  pod_type STRING,
  is_active BOOLEAN,
  commission_rate DECIMAL(5,4),
  team_size INT,
  pod_display_name STRING,
  performance_tier STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer pod dimension with team performance classifications'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_pod AS target
USING (
  SELECT
    sp.pod_key,
    sp.pod_name,
    sp.pod_type,
    sp.is_active,
    sp.commission_rate,
    sp.team_size,
    CASE
      WHEN sp.pod_key = -1 THEN 'Unknown Pod'
      ELSE COALESCE(sp.pod_name, CONCAT('Pod ', sp.pod_key))
    END AS pod_display_name,
    
    -- Performance tier based on commission rate
    CASE
      WHEN sp.commission_rate >= 0.05 THEN 'High Performance (5%+)'
      WHEN sp.commission_rate >= 0.03 THEN 'Good Performance (3-5%)'
      WHEN sp.commission_rate >= 0.01 THEN 'Standard Performance (1-3%)'
      WHEN sp.commission_rate > 0 THEN 'Low Performance (<1%)'
      ELSE 'No Commission'
    END AS performance_tier,
    
    sp._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_pod sp
) AS source
ON target.pod_key = source.pod_key

WHEN MATCHED THEN
  UPDATE SET
    target.pod_name = source.pod_name,
    target.pod_type = source.pod_type,
    target.is_active = source.is_active,
    target.commission_rate = source.commission_rate,
    target.team_size = source.team_size,
    target.pod_display_name = source.pod_display_name,
    target.performance_tier = source.performance_tier,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    pod_key,
    pod_name,
    pod_type,
    is_active,
    commission_rate,
    team_size,
    pod_display_name,
    performance_tier,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.pod_key,
    source.pod_name,
    source.pod_type,
    source.is_active,
    source.commission_rate,
    source.team_size,
    source.pod_display_name,
    source.performance_tier,
    source._source_table,
    source._load_timestamp
  ); 