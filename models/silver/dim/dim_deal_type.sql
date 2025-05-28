-- models/silver/dim/dim_deal_type.sql
-- Silver layer deal type dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_deal_type (
  deal_type_key STRING NOT NULL, -- Natural key from source (type)
  deal_type_description STRING, -- Descriptive name for the deal type
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer deal type dimension storing distinct deal types and their attributes'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_deal_type AS target
USING (
  WITH source_types AS (
    SELECT DISTINCT
      d.type,
      ROW_NUMBER() OVER (PARTITION BY d.type ORDER BY d.type) as rn
    FROM bronze.leaseend_db_public.deals d
    WHERE d.type IS NOT NULL 
      AND d.type <> ''
      AND d._fivetran_deleted = FALSE
  )
  SELECT
    st.type AS deal_type_key,
    -- Map deal types to descriptive names
    CASE
      WHEN st.type = 'acquisition' THEN 'Acquisition'
      WHEN st.type = 'refi' THEN 'Refinance'
      WHEN st.type = 'buyout' THEN 'Lease Buyout'
      WHEN st.type = 'purchase' THEN 'Purchase'
      WHEN st.type = 'lease_end' THEN 'Lease End'
      ELSE COALESCE(st.type, 'Unknown')
    END AS deal_type_description,
    'bronze.leaseend_db_public.deals' AS _source_table
  FROM source_types st
  WHERE st.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as deal_type_key,
    'Unknown' as deal_type_description,
    'system' AS _source_table
) AS source
ON target.deal_type_key = source.deal_type_key

-- Update existing types if description changes
WHEN MATCHED AND (
    target.deal_type_description <> source.deal_type_description
  ) THEN
  UPDATE SET
    target.deal_type_description = source.deal_type_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new types
WHEN NOT MATCHED THEN
  INSERT (
    deal_type_key,
    deal_type_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_type_key,
    source.deal_type_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 