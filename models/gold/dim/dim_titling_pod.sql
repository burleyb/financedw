-- models/gold/dim/dim_titling_pod.sql
-- Gold layer titling pod dimension with business enhancements

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS gold.finance.dim_titling_pod;

CREATE TABLE gold.finance.dim_titling_pod (
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

-- Insert all data from silver with business enhancements
INSERT INTO gold.finance.dim_titling_pod (
  titling_pod_key,
  titling_pod_name,
  is_active,
  team_size,
  specialization,
  titling_pod_display_name,
  _source_table,
  _load_timestamp
)
SELECT
  stp.titling_pod_key,
  stp.name AS titling_pod_name,  -- Silver table uses 'name' column
  TRUE AS is_active,  -- Default to active since silver doesn't have this field
  NULL AS team_size,  -- Silver doesn't have team_size, set to NULL
  
  -- Determine specialization based on name patterns
  CASE
    WHEN UPPER(stp.name) LIKE '%TITLE%' THEN 'Title Processing'
    WHEN UPPER(stp.name) LIKE '%REGISTRATION%' THEN 'Registration Processing'
    WHEN UPPER(stp.name) LIKE '%DMV%' THEN 'DMV Relations'
    WHEN UPPER(stp.name) LIKE '%EXPEDITE%' THEN 'Expedited Processing'
    ELSE 'General Processing'
  END AS specialization,
  
  CASE
    WHEN stp.titling_pod_key = -1 THEN 'Unknown Titling Pod'
    ELSE COALESCE(stp.name, CONCAT('Titling Pod ', stp.titling_pod_key))
  END AS titling_pod_display_name,
  
  COALESCE(stp._source_table, 'silver.finance.dim_titling_pod') AS _source_table,
  CURRENT_TIMESTAMP() AS _load_timestamp
FROM silver.finance.dim_titling_pod stp; 