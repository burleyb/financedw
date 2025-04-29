-- models/dim/dim_bank.sql
-- Dimension table for banks sourced from bronze.leaseend_db_public.banks

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS finance_gold.finance.dim_bank (
  bank_key STRING NOT NULL, -- Natural key from source (bronze.leaseend_db_public.banks.name)
  bank_name STRING, -- Name from source (bronze.leaseend_db_public.banks.name)
  bank_display_name STRING, -- Display name from source (bronze.leaseend_db_public.banks.display_name)
  bank_description STRING, -- Description from source (bronze.leaseend_db_public.banks.description)
  address STRING,
  city STRING,
  state STRING,
  zip STRING,
  phone STRING,
  logo_url STRING,
  is_active BOOLEAN,        -- From bronze.leaseend_db_public.banks.active
  signing_solution STRING,
  min_credit_score INT,
  is_auto_structure BOOLEAN, -- From bronze.leaseend_db_public.banks.auto_structure
  is_auto_structure_refi BOOLEAN,
  is_auto_structure_buyout BOOLEAN,
  -- Add other relevant attributes as needed
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct bank details sourced from bronze.leaseend_db_public.banks.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes from bronze.leaseend_db_public.banks
MERGE INTO finance_gold.finance.dim_bank AS target
USING (
  -- Select relevant columns from the source bank table
  SELECT
    b.name AS bank_key,
    b.name AS bank_name,
    b.display_name AS bank_display_name,
    b.description AS bank_description,
    b.address,
    b.city,
    b.state,
    b.zip,
    b.phone,
    b.logo_url,
    b.active AS is_active,
    b.signing_solution,
    b.min_credit_score,
    b.auto_structure AS is_auto_structure,
    b.auto_structure_refi AS is_auto_structure_refi,
    b.auto_structure_buyout AS is_auto_structure_buyout,
    'bronze.leaseend_db_public.banks' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM bronze.leaseend_db_public.banks b
  WHERE b.name IS NOT NULL AND TRIM(b.name) <> ''
  -- Optional: QUALIFY ROW_NUMBER() OVER (PARTITION BY b.name ORDER BY b.updated_at DESC) = 1 -- If source has duplicates/history
) AS source
ON target.bank_key = source.bank_key

-- Update existing banks if relevant attributes change in the source (SCD Type 1)
WHEN MATCHED AND (
    target.bank_name <> source.bank_name OR
    target.bank_display_name <> source.bank_display_name OR
    target.bank_description <> source.bank_description OR
    target.address <> source.address OR
    target.city <> source.city OR
    target.state <> source.state OR
    target.zip <> source.zip OR
    target.phone <> source.phone OR
    target.logo_url <> source.logo_url OR
    target.is_active <> source.is_active OR
    target.signing_solution <> source.signing_solution OR
    target.min_credit_score <> source.min_credit_score OR
    target.is_auto_structure <> source.is_auto_structure OR
    target.is_auto_structure_refi <> source.is_auto_structure_refi OR
    target.is_auto_structure_buyout <> source.is_auto_structure_buyout
    -- Add comparisons for any other added fields
  ) THEN
  UPDATE SET
    target.bank_name = source.bank_name,
    target.bank_display_name = source.bank_display_name,
    target.bank_description = source.bank_description,
    target.address = source.address,
    target.city = source.city,
    target.state = source.state,
    target.zip = source.zip,
    target.phone = source.phone,
    target.logo_url = source.logo_url,
    target.is_active = source.is_active,
    target.signing_solution = source.signing_solution,
    target.min_credit_score = source.min_credit_score,
    target.is_auto_structure = source.is_auto_structure,
    target.is_auto_structure_refi = source.is_auto_structure_refi,
    target.is_auto_structure_buyout = source.is_auto_structure_buyout,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

-- Insert new banks found in the source
WHEN NOT MATCHED THEN
  INSERT (
    bank_key, bank_name, bank_display_name, bank_description, address, city, state, zip, phone, logo_url,
    is_active, signing_solution, min_credit_score, is_auto_structure,
    is_auto_structure_refi, is_auto_structure_buyout, _source_table, _load_timestamp
  )
  VALUES (
    source.bank_key, source.bank_name, source.bank_display_name, source.bank_description, source.address, source.city,
    source.state, source.zip, source.phone, source.logo_url, source.is_active,
    source.signing_solution, source.min_credit_score, source.is_auto_structure,
    source.is_auto_structure_refi, source.is_auto_structure_buyout,
    source._source_table, source._load_timestamp
  );

-- 3. Ensure 'Unknown' bank exists for referential integrity
MERGE INTO finance_gold.finance.dim_bank AS target
USING (
  SELECT
    'Unknown' as bank_key,
    'Unknown or Not Applicable' as bank_name,
    'Unknown or Not Applicable' as bank_display_name,
    'Unknown or Not Applicable' as bank_description,
    NULL as address, NULL as city, NULL as state, NULL as zip, NULL as phone, NULL as logo_url,
    NULL as is_active, NULL as signing_solution, NULL as min_credit_score, NULL as is_auto_structure,
    NULL as is_auto_structure_refi, NULL as is_auto_structure_buyout,
    'static' as _source_table,
    CAST('1900-01-01' AS TIMESTAMP) as _load_timestamp
) AS source
ON target.bank_key = source.bank_key
WHEN NOT MATCHED THEN INSERT (
    bank_key, bank_name, bank_display_name, bank_description, address, city, state, zip, phone, logo_url,
    is_active, signing_solution, min_credit_score, is_auto_structure,
    is_auto_structure_refi, is_auto_structure_buyout, _source_table, _load_timestamp
  )
  VALUES (
    source.bank_key, source.bank_name, source.bank_display_name, source.bank_description, source.address, source.city,
    source.state, source.zip, source.phone, source.logo_url, source.is_active,
    source.signing_solution, source.min_credit_score, source.is_auto_structure,
    source.is_auto_structure_refi, source.is_auto_structure_buyout,
    source._source_table, source._load_timestamp
  ); 