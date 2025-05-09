-- models/dim/dim_processor.sql
-- Dimension table for deal processors (can be systems, teams, or statuses)

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_processor (
  processor_key STRING NOT NULL,      -- Natural key from source (processor, tax_processor, fee_processor), stored in UPPERCASE
  processor_description STRING,     -- Description derived from key
  -- Add other relevant attributes if known (e.g., processor_type)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct deal processor entities or statuses.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true','delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge distinct processor values with descriptions
MERGE INTO gold.finance.dim_processor AS target
USING (
  SELECT
    processor_val_upper AS processor_key,
    -- Descriptions based on provided sample data values
    CASE processor_val_upper
      WHEN 'VITU' THEN 'VITU Electronic Vehicle Registration & Titling'
      WHEN 'DLRDMV' THEN 'Dealer DMV Service'
      WHEN 'THE TITLE GIRL' THEN 'The Title Girl Service'
      WHEN 'ATC' THEN 'Automated Title and Compliance Service'
      WHEN 'WKLS' THEN 'WKLS Service' -- Assuming WKLS is a known service/system
      WHEN 'STATE DMV' THEN 'State Department of Motor Vehicles'
      WHEN 'OTHER (LEAVE NOTE)' THEN 'Other Processor (See Deal Notes)'
      WHEN 'PLATEMAN' THEN 'Plateman Service' -- Assuming Plateman is a known service/system
      WHEN 'DLR50' THEN 'DLR50 Service' -- Assuming DLR50 is a known service/system
      ELSE processor_val_upper -- Default to the key value if no specific match
    END AS processor_description,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM (
    -- Combine distinct, non-empty, uppercase processor values from all relevant columns
    SELECT DISTINCT processor_val_upper
    FROM (
      SELECT UPPER(processor) AS processor_val_upper FROM silver.deal.big_deal WHERE processor IS NOT NULL AND TRIM(processor) != ''
      UNION
      SELECT UPPER(tax_processor) AS processor_val_upper FROM silver.deal.big_deal WHERE tax_processor IS NOT NULL AND TRIM(tax_processor) != ''
      UNION
      SELECT UPPER(fee_processor) AS processor_val_upper FROM silver.deal.big_deal WHERE fee_processor IS NOT NULL AND TRIM(fee_processor) != ''
    )
    WHERE processor_val_upper IS NOT NULL
  )
) AS source
ON target.processor_key = source.processor_key

-- Insert new processor values
WHEN NOT MATCHED THEN
  INSERT (processor_key, processor_description, _source_table, _load_timestamp)
  VALUES (
    source.processor_key,
    source.processor_description, -- Use the description derived from the CASE statement
    source._source_table,
    source._load_timestamp
  );

-- 3. Add 'Unknown' Processor for referential integrity
MERGE INTO gold.finance.dim_processor AS target
USING (
  SELECT
    'UNKNOWN' as processor_key, -- Use UPPERCASE for consistency
    'Unknown or Not Applicable' as processor_description,
    'static' AS _source_table,
    CAST('1900-01-01' AS TIMESTAMP) AS _load_timestamp
) AS source
ON target.processor_key = source.processor_key
WHEN NOT MATCHED THEN INSERT *; 