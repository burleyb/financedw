-- models/dim/dim_product.sql
-- Dimension table for products (VSC, GAP, etc.)

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.dim_product (
  product_key STRING NOT NULL, -- Composite key or derived key representing the unique product offering
  product_description STRING, -- User-friendly description
  vsc_type STRING, -- FK to dim_vsc_type or directly store the type
  gap_included BOOLEAN, -- Flag if GAP is part of the offering
  -- Add other relevant product attributes if available (e.g., term, provider)
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Dimension table storing distinct product offerings like VSC and GAP.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.dim_product AS target
USING (
  -- Source query: Combine distinct VSC types and GAP presence
  -- This logic might need refinement based on how products are truly represented
  WITH source_products AS (
      SELECT DISTINCT
          COALESCE(vsc_type, 'N/A') as vsc_type_val,
          CASE
              WHEN option_type IN ('vscPlusGap', 'gap') THEN true
              ELSE false
          END as gap_included_flag
      FROM silver.deal.big_deal
      WHERE option_type IS NOT NULL -- Ensure we have some product context
  )
  SELECT
    -- Create a composite key (adjust separator if needed)
    CONCAT(s.vsc_type_val, '|', CAST(s.gap_included_flag AS STRING)) as product_key,
    -- Derive description
    CASE
      WHEN s.vsc_type_val <> 'N/A' AND s.gap_included_flag THEN CONCAT('VSC (', s.vsc_type_val, ') + GAP')
      WHEN s.vsc_type_val <> 'N/A' AND NOT s.gap_included_flag THEN CONCAT('VSC (', s.vsc_type_val, ') Only')
      WHEN s.vsc_type_val = 'N/A' AND s.gap_included_flag THEN 'GAP Only'
      ELSE 'Other/Unknown Product' -- Should ideally not happen with WHERE clause
    END AS product_description,
    CASE WHEN s.vsc_type_val = 'N/A' THEN NULL ELSE s.vsc_type_val END as vsc_type, -- Store actual VSC type or NULL
    s.gap_included_flag as gap_included,
    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM source_products s
) AS source
ON target.product_key = source.product_key

-- Update existing products if description or attributes change
WHEN MATCHED AND (
    target.product_description <> source.product_description OR
    COALESCE(target.vsc_type, '') <> COALESCE(source.vsc_type, '') OR
    target.gap_included <> source.gap_included
  ) THEN
  UPDATE SET
    target.product_description = source.product_description,
    target.vsc_type = source.vsc_type,
    target.gap_included = source.gap_included,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new products
WHEN NOT MATCHED THEN
  INSERT (product_key, product_description, vsc_type, gap_included, _source_table, _load_timestamp)
  VALUES (source.product_key, source.product_description, source.vsc_type, source.gap_included, source._source_table, CURRENT_TIMESTAMP());

-- Ensure 'Unknown' and 'No Products' entries exist
MERGE INTO gold.finance.dim_product AS target
USING (
    SELECT 'Unknown' as product_key, 'Unknown Product' as product_description, NULL as vsc_type, NULL as gap_included, 'static' as _source_table
    UNION ALL
    SELECT 'No Products' as product_key, 'No Products' as product_description, NULL as vsc_type, false as gap_included, 'static' as _source_table

) AS source
ON target.product_key = source.product_key
WHEN NOT MATCHED THEN INSERT (product_key, product_description, vsc_type, gap_included, _source_table, _load_timestamp)
VALUES (source.product_key, source.product_description, source.vsc_type, source.gap_included, source._source_table, current_timestamp());

-- Note: This DDL structure is refined but requires detailed ETL logic.
-- Populating it requires an ETL process that handles:
-- 1. Sourcing from silver.deal.big_deal, bronze.leaseend_db_public.financial_infos, and potentially bronze.leaseend_db_public.products.
-- 2. Deriving product_category, product_name, term_months/miles.
-- 3. Creating a stable product_source_identifier.
-- 4. Implementing SCD Type 2 logic.
-- 5. Handling NULLs and data cleansing (e.g., parsing term strings). 