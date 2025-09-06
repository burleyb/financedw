-- models/gold/dim/dim_employee_pod.sql
-- Gold layer employee pod bridge table with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_employee_pod;

CREATE TABLE gold.finance.dim_employee_pod (
  employee_key STRING NOT NULL,
  pod_key INT NOT NULL,
  relationship_status STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer employee pod bridge table with relationship status'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_employee_pod (
  employee_key,
  pod_key,
  relationship_status,
  _source_table,
  _load_timestamp
)
SELECT
  sep.employee_key,
  sep.pod_key,
  'Active' AS relationship_status,  -- All relationships in the bridge are active
  sep._source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_employee_pod sep; 