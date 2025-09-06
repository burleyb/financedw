-- models/gold/dim/dim_employee_titling_pod.sql
-- Gold layer employee titling pod bridge table with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_employee_titling_pod;

CREATE TABLE gold.finance.dim_employee_titling_pod (
  employee_key STRING NOT NULL,
  titling_pod_key INT NOT NULL,
  relationship_status STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer employee titling pod bridge table with relationship status'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_employee_titling_pod (
  employee_key,
  titling_pod_key,
  relationship_status,
  _source_table,
  _load_timestamp
)
SELECT
  setp.employee_key,
  setp.titling_pod_key,
  'Active' AS relationship_status,  -- All relationships in the bridge are active
  setp._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_employee_titling_pod setp; 