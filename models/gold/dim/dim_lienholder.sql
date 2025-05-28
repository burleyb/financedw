-- models/gold/dim/dim_lienholder.sql
-- Gold layer lienholder dimension with business enhancements

CREATE TABLE IF NOT EXISTS gold.finance.dim_lienholder (
  lienholder_key STRING NOT NULL,
  lienholder_name STRING,
  lienholder_slug STRING,
  lienholder_type STRING,
  is_major_lienholder BOOLEAN,
  lienholder_display_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer lienholder dimension with business classifications'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_lienholder AS target
USING (
  SELECT
    sl.lienholder_key,
    sl.lienholder_name,
    sl.lienholder_slug,
    
    -- Categorize lienholder types
    CASE
      WHEN UPPER(sl.lienholder_name) LIKE '%BANK%' THEN 'Bank'
      WHEN UPPER(sl.lienholder_name) LIKE '%CREDIT UNION%' THEN 'Credit Union'
      WHEN UPPER(sl.lienholder_name) LIKE '%FINANCIAL%' THEN 'Financial Services'
      WHEN UPPER(sl.lienholder_name) LIKE '%LEASE%' OR UPPER(sl.lienholder_name) LIKE '%LEASING%' THEN 'Leasing Company'
      WHEN UPPER(sl.lienholder_name) LIKE '%MOTOR%' OR UPPER(sl.lienholder_name) LIKE '%AUTO%' THEN 'Auto Finance'
      WHEN sl.lienholder_name = 'Unknown' THEN 'Unknown'
      ELSE 'Other'
    END AS lienholder_type,
    
    -- Flag major lienholders (common ones)
    CASE
      WHEN UPPER(sl.lienholder_name) LIKE '%ALLY%' OR 
           UPPER(sl.lienholder_name) LIKE '%WELLS FARGO%' OR
           UPPER(sl.lienholder_name) LIKE '%CHASE%' OR
           UPPER(sl.lienholder_name) LIKE '%BANK OF AMERICA%' OR
           UPPER(sl.lienholder_name) LIKE '%CAPITAL ONE%' OR
           UPPER(sl.lienholder_name) LIKE '%SANTANDER%' OR
           UPPER(sl.lienholder_name) LIKE '%TOYOTA%' OR
           UPPER(sl.lienholder_name) LIKE '%HONDA%' OR
           UPPER(sl.lienholder_name) LIKE '%FORD%' OR
           UPPER(sl.lienholder_name) LIKE '%GM%' THEN TRUE
      ELSE FALSE
    END AS is_major_lienholder,
    
    CASE
      WHEN sl.lienholder_key = 'Unknown' THEN 'Unknown Lienholder'
      ELSE COALESCE(sl.lienholder_name, sl.lienholder_key, 'Unknown')
    END AS lienholder_display_name,
    
    sl._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_lienholder sl
) AS source
ON target.lienholder_key = source.lienholder_key

WHEN MATCHED THEN
  UPDATE SET
    target.lienholder_name = source.lienholder_name,
    target.lienholder_slug = source.lienholder_slug,
    target.lienholder_type = source.lienholder_type,
    target.is_major_lienholder = source.is_major_lienholder,
    target.lienholder_display_name = source.lienholder_display_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    lienholder_key,
    lienholder_name,
    lienholder_slug,
    lienholder_type,
    is_major_lienholder,
    lienholder_display_name,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.lienholder_key,
    source.lienholder_name,
    source.lienholder_slug,
    source.lienholder_type,
    source.is_major_lienholder,
    source.lienholder_display_name,
    source._source_table,
    source._load_timestamp
  ); 