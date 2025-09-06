-- models/silver/dim/dim_processor.sql
-- Silver layer processor dimension table reading from bronze sources

-- Drop and recreate table to ensure schema consistency
DROP TABLE IF EXISTS silver.finance.dim_processor;

CREATE TABLE silver.finance.dim_processor (
  processor_key STRING NOT NULL, -- Natural key from source (processor, tax_processor, fee_processor), stored in UPPERCASE
  processor_description STRING, -- Description derived from key
  processor_type STRING, -- Type of processor (general, tax, fee)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer processor dimension storing distinct deal processor entities or statuses'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Insert all processor data from bronze financial_infos table
INSERT INTO silver.finance.dim_processor (
  processor_key,
  processor_description,
  processor_type,
  _source_table,
  _load_timestamp
)
WITH source_processors AS (
  -- Combine distinct, non-empty, uppercase processor values from financial_infos
  SELECT DISTINCT 
    UPPER(TRIM(processor)) AS processor_val_upper,
    'general' as processor_type
  FROM bronze.leaseend_db_public.financial_infos 
  WHERE processor IS NOT NULL 
    AND TRIM(processor) != ''
    AND _fivetran_deleted = FALSE
  
  UNION
  
  SELECT DISTINCT 
    UPPER(TRIM(tax_processor)) AS processor_val_upper,
    'tax' as processor_type
  FROM bronze.leaseend_db_public.financial_infos 
  WHERE tax_processor IS NOT NULL 
    AND TRIM(tax_processor) != ''
    AND _fivetran_deleted = FALSE
  
  UNION
  
  SELECT DISTINCT 
    UPPER(TRIM(fee_processor)) AS processor_val_upper,
    'fee' as processor_type
  FROM bronze.leaseend_db_public.financial_infos 
  WHERE fee_processor IS NOT NULL 
    AND TRIM(fee_processor) != ''
    AND _fivetran_deleted = FALSE
)
SELECT
  sp.processor_val_upper AS processor_key,
  -- Descriptions based on known processor values
  CASE sp.processor_val_upper
    WHEN 'VITU' THEN 'VITU Electronic Vehicle Registration & Titling'
    WHEN 'DLRDMV' THEN 'Dealer DMV Service'
    WHEN 'THE TITLE GIRL' THEN 'The Title Girl Service'
    WHEN 'ATC' THEN 'Automated Title and Compliance Service'
    WHEN 'WKLS' THEN 'WKLS Service'
    WHEN 'STATE DMV' THEN 'State Department of Motor Vehicles'
    WHEN 'OTHER (LEAVE NOTE)' THEN 'Other Processor (See Deal Notes)'
    WHEN 'PLATEMAN' THEN 'Plateman Service'
    WHEN 'DLR50' THEN 'DLR50 Service'
    WHEN 'MANUAL' THEN 'Manual Processing'
    WHEN 'INTERNAL' THEN 'Internal Processing'
    ELSE sp.processor_val_upper -- Default to the key value if no specific match
  END AS processor_description,
  sp.processor_type,
  'bronze.leaseend_db_public.financial_infos' as _source_table,
  CURRENT_TIMESTAMP() as _load_timestamp
FROM source_processors sp
WHERE sp.processor_val_upper IS NOT NULL

UNION ALL

-- Add standard unknown record
SELECT
  'UNKNOWN' as processor_key,
  'Unknown or Not Applicable' as processor_description,
  'unknown' as processor_type,
  'system' AS _source_table,
  CURRENT_TIMESTAMP() as _load_timestamp; 