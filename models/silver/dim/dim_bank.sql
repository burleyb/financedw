-- models/silver/dim/dim_bank.sql
-- Dimension table for banks, sourced from bronze.leaseend_db_public.banks

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS silver.finance.dim_bank (
  bank_key STRING NOT NULL, -- Natural key (bank name/slug)
  bank_id INT, -- Original bank ID
  bank_name STRING,
  bank_slug STRING,
  is_active BOOLEAN,
  _source_table STRING, -- Metadata: Originating source table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO silver.finance.dim_bank AS target
USING (
  -- Select the latest distinct bank data from the bronze banks table
  SELECT  
    COALESCE(slug, CAST(id AS STRING), 'Unknown') AS bank_key, -- Natural Key
    id as bank_id,
    COALESCE(name, 'Unknown') AS bank_name,
    COALESCE(slug, 'unknown') AS bank_slug,
    COALESCE(is_active, false) AS is_active,
    'bronze.leaseend_db_public.banks' AS _source_table
  FROM bronze.leaseend_db_public.banks
  WHERE id IS NOT NULL -- Ensure we have a valid key
  -- Deduplicate based on ID, taking the most recently updated record
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
) AS source
ON target.bank_key = source.bank_key

-- Update existing banks if their data has changed (SCD Type 1)
WHEN MATCHED AND (
    target.bank_id <> source.bank_id OR
    target.bank_name <> source.bank_name OR
    target.bank_slug <> source.bank_slug OR
    target.is_active <> source.is_active
  ) THEN
  UPDATE SET
    target.bank_id = source.bank_id,
    target.bank_name = source.bank_name,
    target.bank_slug = source.bank_slug,
    target.is_active = source.is_active,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new banks
WHEN NOT MATCHED THEN
  INSERT (
    bank_key,
    bank_id,
    bank_name,
    bank_slug,
    is_active,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.bank_key,
    source.bank_id,
    source.bank_name,
    source.bank_slug,
    source.is_active,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- Ensure 'No Bank' and 'Unknown' types exist for handling NULLs
MERGE INTO silver.finance.dim_bank AS target
USING (
  SELECT 'No Bank' as bank_key, NULL as bank_id, 'No Bank' as bank_name, 'no-bank' as bank_slug, false as is_active, 'static' as _source_table
  UNION ALL
  SELECT 'Unknown' as bank_key, NULL as bank_id, 'Unknown' as bank_name, 'unknown' as bank_slug, false as is_active, 'static' as _source_table
) AS source
ON target.bank_key = source.bank_key
WHEN NOT MATCHED THEN 
  INSERT (
    bank_key, 
    bank_id,
    bank_name, 
    bank_slug, 
    is_active,
    _source_table, 
    _load_timestamp
  )
  VALUES (
    source.bank_key, 
    source.bank_id,
    source.bank_name, 
    source.bank_slug, 
    source.is_active,
    source._source_table, 
    CURRENT_TIMESTAMP()
  ); 