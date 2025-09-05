-- models/silver/dim/dim_bank.sql
-- Dimension table for banks, sourced from bronze.leaseend_db_public.banks

-- 1. Drop and recreate table to ensure schema matches
DROP TABLE IF EXISTS silver.finance.dim_bank;

CREATE TABLE silver.finance.dim_bank (
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
  _source_table STRING, -- Metadata: Originating source table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
COMMENT 'Dimension table storing distinct bank details sourced from bronze.leaseend_db_public.banks.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);


-- 2. Insert all bank data from bronze.leaseend_db_public.banks
INSERT INTO silver.finance.dim_bank (
  bank_key,
  bank_name,
  bank_display_name,
  bank_description,
  address,
  city,
  state,
  zip,
  phone,
  logo_url,
  is_active,
  signing_solution,
  min_credit_score,
  is_auto_structure,
  is_auto_structure_refi,
  is_auto_structure_buyout,
  _source_table,
  _load_timestamp
)
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
-- Deduplicate based on name, taking the most recently updated record
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.name ORDER BY b.updated_at DESC) = 1;

-- 3. Insert static 'No Bank' and 'Unknown' records for handling NULLs
INSERT INTO silver.finance.dim_bank (
  bank_key, 
  bank_name,
  bank_display_name,
  bank_description,
  address,
  city,
  state,
  zip,
  phone,
  logo_url,
  is_active,
  signing_solution,
  min_credit_score,
  is_auto_structure,
  is_auto_structure_refi,
  is_auto_structure_buyout,
  _source_table, 
  _load_timestamp
)
VALUES 
  (
    'No Bank', 
    'No Bank',
    'No Bank',
    'Default bank for records with no bank assigned',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    false,
    false,
    false,
    'static', 
    CURRENT_TIMESTAMP()
  ),
  (
    'Unknown', 
    'Unknown',
    'Unknown',
    'Default bank for records with unknown bank',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    false,
    false,
    false,
    'static', 
    CURRENT_TIMESTAMP()
  ); 