-- models/silver/dim/dim_pay_frequency.sql
-- Silver layer pay frequency dimension table reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.dim_pay_frequency (
  pay_frequency_key STRING NOT NULL, -- Natural key from source
  pay_frequency_description STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer pay frequency dimension storing distinct pay frequencies'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze source
MERGE INTO silver.finance.dim_pay_frequency AS target
USING (
  WITH source_frequencies AS (
    SELECT DISTINCT
      e.pay_frequency,
      ROW_NUMBER() OVER (PARTITION BY e.pay_frequency ORDER BY e.pay_frequency) as rn
    FROM bronze.leaseend_db_public.employments e
    WHERE e.pay_frequency IS NOT NULL 
      AND e.pay_frequency <> ''
      AND e._fivetran_deleted = FALSE
  )
  SELECT
    sf.pay_frequency AS pay_frequency_key,
    -- Map pay frequency to descriptive names
    CASE
      WHEN UPPER(sf.pay_frequency) = 'WEEKLY' THEN 'Weekly'
      WHEN UPPER(sf.pay_frequency) = 'BIWEEKLY' THEN 'Bi-Weekly'
      WHEN UPPER(sf.pay_frequency) = 'SEMIMONTHLY' THEN 'Semi-Monthly'
      WHEN UPPER(sf.pay_frequency) = 'MONTHLY' THEN 'Monthly'
      WHEN UPPER(sf.pay_frequency) = 'QUARTERLY' THEN 'Quarterly'
      WHEN UPPER(sf.pay_frequency) = 'ANNUALLY' THEN 'Annually'
      ELSE COALESCE(sf.pay_frequency, 'Unknown')
    END AS pay_frequency_description,
    'bronze.leaseend_db_public.employments' AS _source_table
  FROM source_frequencies sf
  WHERE sf.rn = 1
  
  UNION ALL
  
  -- Add standard unknown record
  SELECT
    'Unknown' as pay_frequency_key,
    'Unknown' as pay_frequency_description,
    'system' AS _source_table
) AS source
ON target.pay_frequency_key = source.pay_frequency_key

-- Update existing frequencies if description changes
WHEN MATCHED AND (
    target.pay_frequency_description <> source.pay_frequency_description
  ) THEN
  UPDATE SET
    target.pay_frequency_description = source.pay_frequency_description,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new frequencies
WHEN NOT MATCHED THEN
  INSERT (
    pay_frequency_key,
    pay_frequency_description,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.pay_frequency_key,
    source.pay_frequency_description,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 