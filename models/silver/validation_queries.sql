-- models/silver/validation_queries.sql
-- Comprehensive validation queries to compare bronze → silver → gold data lineage
-- Note: This assumes the gold_big_deal tables are the EXISTING tables currently in production
-- and the new gold tables will be created separately for comparison

-- 1. Record Count Comparison Across All Layers
WITH bronze_deals AS (
  SELECT COUNT(DISTINCT d.id) as deal_count
  FROM bronze.leaseend_db_public.deals d
  WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
    AND d.state IS NOT NULL 
    AND d.id != 0
)
SELECT 
  'bronze' as source, 
  deal_count as record_count,
  deal_count as unique_deals
FROM bronze_deals
UNION ALL
SELECT 
  'silver' as source, 
  COUNT(*) as record_count,
  COUNT(DISTINCT deal_key) as unique_deals
FROM silver.finance.fact_deals
UNION ALL
SELECT 
  'gold_existing' as source, 
  COUNT(*) as record_count,
  COUNT(DISTINCT deal_key) as unique_deals
FROM gold.finance.fact_deals; -- This is the EXISTING table (from gold_big_deal approach)

-- Note: Add this query once the new gold tables are created:
-- UNION ALL
-- SELECT 
--   'gold_new' as source, 
--   COUNT(*) as record_count,
--   COUNT(DISTINCT deal_key) as unique_deals
-- FROM gold.finance.fact_deals_new; -- This would be the NEW table

-- 2. Deal Key Overlap Analysis Across All Layers
WITH bronze_deals AS (
  SELECT DISTINCT CAST(d.id AS STRING) as deal_key 
  FROM bronze.leaseend_db_public.deals d
  WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
    AND d.state IS NOT NULL 
    AND d.id != 0
),
silver_deals AS (
  SELECT DISTINCT deal_key FROM silver.finance.fact_deals
),
gold_existing_deals AS (
  SELECT DISTINCT deal_key FROM gold.finance.fact_deals -- EXISTING table
)
SELECT 
  'only_in_bronze' as category,
  COUNT(*) as deal_count
FROM bronze_deals b
LEFT JOIN silver_deals s ON b.deal_key = s.deal_key
LEFT JOIN gold_existing_deals ge ON b.deal_key = ge.deal_key
WHERE s.deal_key IS NULL AND ge.deal_key IS NULL
UNION ALL
SELECT 
  'only_in_silver' as category,
  COUNT(*) as deal_count
FROM silver_deals s
LEFT JOIN bronze_deals b ON s.deal_key = b.deal_key
LEFT JOIN gold_existing_deals ge ON s.deal_key = ge.deal_key
WHERE b.deal_key IS NULL AND ge.deal_key IS NULL
UNION ALL
SELECT 
  'only_in_gold_existing' as category,
  COUNT(*) as deal_count
FROM gold_existing_deals ge
LEFT JOIN bronze_deals b ON ge.deal_key = b.deal_key
LEFT JOIN silver_deals s ON ge.deal_key = s.deal_key
WHERE b.deal_key IS NULL AND s.deal_key IS NULL
UNION ALL
SELECT 
  'bronze_silver_gold_existing' as category,
  COUNT(*) as deal_count
FROM bronze_deals b
INNER JOIN silver_deals s ON b.deal_key = s.deal_key
INNER JOIN gold_existing_deals ge ON b.deal_key = ge.deal_key;

-- 3. Financial Measures Comparison with Bronze Source Values
WITH latest_financial_infos AS (
  SELECT 
    deal_id,
    amount_financed,
    profit,
    vsc_price,
    vsc_cost,
    gap_price,
    gap_cost,
    title,
    doc_fee,
    option_type,
    -- Calculate bronze-level derived values
    CASE 
      WHEN option_type IN ('vsc', 'vscPlusGap') THEN COALESCE(vsc_price, 0) - COALESCE(vsc_cost, 0)
      ELSE 0
    END AS vsc_rev,
    CASE 
      WHEN option_type IN ('gap', 'vscPlusGap') THEN COALESCE(gap_price, 0) - COALESCE(gap_cost, 0)
      ELSE 0
    END AS gap_rev,
    ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY updated_at DESC) as rn
  FROM bronze.leaseend_db_public.financial_infos
  WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
)
SELECT 
  ge.deal_key,
  -- Bronze source values
  b.amount_financed as bronze_amount_financed,
  b.profit as bronze_profit,
  b.vsc_rev as bronze_vsc_rev,
  b.gap_rev as bronze_gap_rev,
  COALESCE(b.title, 0) + COALESCE(b.doc_fee, 0) + COALESCE(b.profit, 0) as bronze_rpt,
  -- Silver values (convert from cents back to dollars for readability)
  s.amount_financed_amount / 100.0 as silver_amount_financed,
  s.profit_amount / 100.0 as silver_profit,
  s.vsc_rev_amount / 100.0 as silver_vsc_rev,
  s.gap_rev_amount / 100.0 as silver_gap_rev,
  s.rpt_amount / 100.0 as silver_rpt,
  -- Gold Existing values (convert from cents back to dollars for readability)
  ge.amount_financed_amount / 100.0 as gold_existing_amount_financed,
  ge.profit_amount / 100.0 as gold_existing_profit,
  ge.vsc_rev_amount / 100.0 as gold_existing_vsc_rev,
  ge.gap_rev_amount / 100.0 as gold_existing_gap_rev,
  ge.rpt_amount / 100.0 as gold_existing_rpt,
  -- Bronze to Silver differences
  (s.amount_financed_amount / 100.0) - COALESCE(b.amount_financed, 0) as bronze_silver_amount_financed_diff,
  (s.profit_amount / 100.0) - COALESCE(b.profit, 0) as bronze_silver_profit_diff,
  (s.vsc_rev_amount / 100.0) - COALESCE(b.vsc_rev, 0) as bronze_silver_vsc_rev_diff,
  (s.gap_rev_amount / 100.0) - COALESCE(b.gap_rev, 0) as bronze_silver_gap_rev_diff,
  (s.rpt_amount / 100.0) - (COALESCE(b.title, 0) + COALESCE(b.doc_fee, 0) + COALESCE(b.profit, 0)) as bronze_silver_rpt_diff,
  -- Silver to Gold Existing differences
  (ge.amount_financed_amount - s.amount_financed_amount) / 100.0 as silver_gold_existing_amount_financed_diff,
  (ge.profit_amount - s.profit_amount) / 100.0 as silver_gold_existing_profit_diff,
  (ge.vsc_rev_amount - s.vsc_rev_amount) / 100.0 as silver_gold_existing_vsc_rev_diff,
  (ge.gap_rev_amount - s.gap_rev_amount) / 100.0 as silver_gold_existing_gap_rev_diff,
  (ge.rpt_amount - s.rpt_amount) / 100.0 as silver_gold_existing_rpt_diff
FROM gold.finance.fact_deals ge -- EXISTING table
LEFT JOIN silver.finance.fact_deals s ON ge.deal_key = s.deal_key
LEFT JOIN latest_financial_infos b ON CAST(ge.deal_key AS INT) = b.deal_id AND b.rn = 1
WHERE 
  -- Any differences between layers
  (s.deal_key IS NOT NULL AND (
    ABS((s.amount_financed_amount / 100.0) - COALESCE(b.amount_financed, 0)) > 0.01
    OR ABS((s.profit_amount / 100.0) - COALESCE(b.profit, 0)) > 0.01
    OR ABS((s.vsc_rev_amount / 100.0) - COALESCE(b.vsc_rev, 0)) > 0.01
    OR ABS((s.gap_rev_amount / 100.0) - COALESCE(b.gap_rev, 0)) > 0.01
    OR ABS((ge.amount_financed_amount - s.amount_financed_amount) / 100.0) > 0.01
    OR ABS((ge.profit_amount - s.profit_amount) / 100.0) > 0.01
    OR ABS((ge.vsc_rev_amount - s.vsc_rev_amount) / 100.0) > 0.01
    OR ABS((ge.gap_rev_amount - s.gap_rev_amount) / 100.0) > 0.01
  ))
ORDER BY ABS(COALESCE((ge.profit_amount - COALESCE(s.profit_amount, 0)) / 100.0, 0)) DESC
LIMIT 100;

-- 4. Key Dimension Comparison Across All Layers
WITH bronze_deal_info AS (
  SELECT 
    d.id as deal_id,
    d.state as deal_state,
    d.type as deal_type,
    fi.option_type,
    fi.bank,
    ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY COALESCE(fi.updated_at, d.updated_at) DESC) as rn
  FROM bronze.leaseend_db_public.deals d
  LEFT JOIN bronze.leaseend_db_public.financial_infos fi ON d.id = fi.deal_id 
    AND (fi._fivetran_deleted = FALSE OR fi._fivetran_deleted IS NULL)
  WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
    AND d.state IS NOT NULL 
    AND d.id != 0
)
SELECT 
  g.deal_key,
  -- Bronze values
  b.deal_state as bronze_deal_state,
  b.deal_type as bronze_deal_type,
  b.option_type as bronze_option_type,
  b.bank as bronze_bank,
  -- Silver values
  s.deal_state_key as silver_deal_state,
  s.deal_type_key as silver_deal_type,
  s.option_type_key as silver_option_type,
  s.bank_key as silver_bank,
  -- Gold values
  g.deal_state_key as gold_deal_state,
  g.deal_type_key as gold_deal_type,
  g.option_type_key as gold_option_type,
  g.bank_key as gold_bank
FROM gold.finance.fact_deals g
INNER JOIN silver.finance.fact_deals s ON g.deal_key = s.deal_key
LEFT JOIN bronze_deal_info b ON CAST(g.deal_key AS INT) = b.deal_id AND b.rn = 1
WHERE 
  -- Differences between any layers
  g.deal_state_key != s.deal_state_key
  OR g.deal_type_key != s.deal_type_key
  OR g.option_type_key != s.option_type_key
  OR g.bank_key != s.bank_key
  OR COALESCE(s.deal_state_key, 'Unknown') != COALESCE(b.deal_state, 'Unknown')
  OR COALESCE(s.deal_type_key, 'Unknown') != COALESCE(b.deal_type, 'Unknown')
  OR COALESCE(s.option_type_key, 'noProducts') != COALESCE(b.option_type, 'noProducts')
  OR COALESCE(s.bank_key, 'No Bank') != COALESCE(b.bank, 'No Bank')
LIMIT 100;

-- 5. Summary Statistics Comparison Across All Layers
WITH bronze_summary AS (
  SELECT 
    COUNT(DISTINCT d.id) as total_deals,
    SUM(COALESCE(fi.amount_financed, 0)) as total_amount_financed,
    SUM(COALESCE(fi.profit, 0)) as total_profit,
    SUM(CASE 
      WHEN fi.option_type IN ('vsc', 'vscPlusGap') 
      THEN COALESCE(fi.vsc_price, 0) - COALESCE(fi.vsc_cost, 0)
      ELSE 0 
    END) as total_vsc_revenue,
    SUM(CASE 
      WHEN fi.option_type IN ('gap', 'vscPlusGap') 
      THEN COALESCE(fi.gap_price, 0) - COALESCE(fi.gap_cost, 0)
      ELSE 0 
    END) as total_gap_revenue,
    AVG(COALESCE(fi.amount_financed, 0)) as avg_amount_financed,
    AVG(COALESCE(fi.profit, 0)) as avg_profit
  FROM bronze.leaseend_db_public.deals d
  LEFT JOIN (
    SELECT deal_id, amount_financed, profit, vsc_price, vsc_cost, gap_price, gap_cost, option_type,
           ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY updated_at DESC) as rn
    FROM bronze.leaseend_db_public.financial_infos
    WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
  ) fi ON d.id = fi.deal_id AND fi.rn = 1
  WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
    AND d.state IS NOT NULL 
    AND d.id != 0
)
SELECT 
  'bronze' as source,
  total_deals,
  total_amount_financed,
  total_profit,
  total_vsc_revenue,
  total_gap_revenue,
  avg_amount_financed,
  avg_profit
FROM bronze_summary
UNION ALL
SELECT 
  'silver' as source,
  COUNT(*) as total_deals,
  SUM(amount_financed_amount) / 100.0 as total_amount_financed,
  SUM(profit_amount) / 100.0 as total_profit,
  SUM(vsc_rev_amount) / 100.0 as total_vsc_revenue,
  SUM(gap_rev_amount) / 100.0 as total_gap_revenue,
  AVG(amount_financed_amount) / 100.0 as avg_amount_financed,
  AVG(profit_amount) / 100.0 as avg_profit
FROM silver.finance.fact_deals
UNION ALL
SELECT 
  'gold_existing' as source,
  COUNT(*) as total_deals,
  SUM(amount_financed_amount) / 100.0 as total_amount_financed,
  SUM(profit_amount) / 100.0 as total_profit,
  SUM(vsc_rev_amount) / 100.0 as total_vsc_revenue,
  SUM(gap_rev_amount) / 100.0 as total_gap_revenue,
  AVG(amount_financed_amount) / 100.0 as avg_amount_financed,
  AVG(profit_amount) / 100.0 as avg_profit
FROM gold.finance.fact_deals; -- EXISTING table

-- 6. Date Range Comparison Across All Layers
WITH bronze_dates AS (
  SELECT 
    MIN(CAST(DATE_FORMAT(d.creation_date_utc, 'yyyyMMdd') AS INT)) as min_creation_date,
    MAX(CAST(DATE_FORMAT(d.creation_date_utc, 'yyyyMMdd') AS INT)) as max_creation_date,
    MIN(CAST(DATE_FORMAT(d.completion_date_utc, 'yyyyMMdd') AS INT)) as min_completion_date,
    MAX(CAST(DATE_FORMAT(d.completion_date_utc, 'yyyyMMdd') AS INT)) as max_completion_date
  FROM bronze.leaseend_db_public.deals d
  WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
    AND d.state IS NOT NULL 
    AND d.id != 0
    AND d.creation_date_utc IS NOT NULL
)
SELECT 
  'bronze' as source,
  min_creation_date,
  max_creation_date,
  min_completion_date,
  max_completion_date
FROM bronze_dates
UNION ALL
SELECT 
  'silver' as source,
  MIN(creation_date_key) as min_creation_date,
  MAX(creation_date_key) as max_creation_date,
  MIN(completion_date_key) as min_completion_date,
  MAX(completion_date_key) as max_completion_date
FROM silver.finance.fact_deals
WHERE creation_date_key > 0
UNION ALL
SELECT 
  'gold_existing' as source,
  MIN(creation_date_key) as min_creation_date,
  MAX(creation_date_key) as max_creation_date,
  MIN(completion_date_key) as min_completion_date,
  MAX(completion_date_key) as max_completion_date
FROM gold.finance.fact_deals
WHERE creation_date_key > 0;

-- 7. Dimension Table Comparison Across All Layers
WITH bronze_dim_summary AS (
  SELECT 
    COUNT(DISTINCT d.id) as total_deals,
    COUNT(DISTINCT d.state) as unique_deal_states,
    COUNT(DISTINCT d.type) as unique_deal_types,
    COUNT(DISTINCT fi.option_type) as unique_option_types
  FROM bronze.leaseend_db_public.deals d
  LEFT JOIN (
    SELECT deal_id, option_type,
           ROW_NUMBER() OVER (PARTITION BY deal_id ORDER BY updated_at DESC) as rn
    FROM bronze.leaseend_db_public.financial_infos
    WHERE _fivetran_deleted = FALSE OR _fivetran_deleted IS NULL
  ) fi ON d.id = fi.deal_id AND fi.rn = 1
  WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
    AND d.state IS NOT NULL 
    AND d.id != 0
)
SELECT 
  'bronze' as source,
  total_deals,
  unique_deal_states,
  unique_deal_types,
  unique_option_types
FROM bronze_dim_summary
UNION ALL
SELECT 
  'silver' as source,
  COUNT(*) as total_deals,
  COUNT(DISTINCT deal_state_key) as unique_deal_states,
  COUNT(DISTINCT deal_type_key) as unique_deal_types,
  COUNT(DISTINCT option_type_key) as unique_option_types
FROM silver.finance.dim_deal
UNION ALL
SELECT 
  'gold_existing' as source,
  COUNT(*) as total_deals,
  COUNT(DISTINCT deal_state_key) as unique_deal_states,
  COUNT(DISTINCT deal_type_key) as unique_deal_types,
  COUNT(DISTINCT option_type_key) as unique_option_types
FROM gold.finance.dim_deal;

-- 8. Silver-Specific Attributes Analysis
-- This query shows the additional attributes available in the silver approach
SELECT 
  COUNT(*) as total_deals,
  COUNT(imported_date_utc) as has_imported_date,
  COUNT(auto_import_variation) as has_auto_import_variation,
  COUNT(paperwork_type) as has_paperwork_type,
  COUNT(marketing_source) as has_marketing_source,
  COUNT(lease_id) as has_lease_id,
  SUM(CASE WHEN signing_on_com = TRUE THEN 1 ELSE 0 END) as signing_on_com_count,
  SUM(CASE WHEN sales_visibility = TRUE THEN 1 ELSE 0 END) as sales_visibility_count,
  SUM(CASE WHEN has_problem = TRUE THEN 1 ELSE 0 END) as has_problem_count
FROM silver.finance.dim_deal;

-- 9. Gold-Specific Enhancements Analysis
-- This query shows the business enhancements added in the gold layer
SELECT 
  deal_category,
  processing_complexity,
  customer_segment,
  COUNT(*) as deal_count,
  AVG(total_revenue_amount) / 100.0 as avg_total_revenue,
  AVG(margin_percentage) as avg_margin_percentage,
  SUM(CASE WHEN is_complete_deal = TRUE THEN 1 ELSE 0 END) as complete_deals,
  SUM(CASE WHEN has_financial_products = TRUE THEN 1 ELSE 0 END) as deals_with_products
FROM gold.finance.fact_deals f
JOIN gold.finance.dim_deal d ON f.deal_key = d.deal_key
GROUP BY deal_category, processing_complexity, customer_segment
ORDER BY deal_category, processing_complexity, customer_segment;

-- 10. Data Quality Check - Missing Financial Info Across Layers
SELECT 
  'bronze_deals_without_financial_info' as metric,
  COUNT(*) as count
FROM bronze.leaseend_db_public.deals d
LEFT JOIN bronze.leaseend_db_public.financial_infos fi ON d.id = fi.deal_id 
  AND (fi._fivetran_deleted = FALSE OR fi._fivetran_deleted IS NULL)
WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
  AND d.state IS NOT NULL 
  AND d.id != 0
  AND fi.deal_id IS NULL
UNION ALL
SELECT 
  'silver_deals_without_financial_info' as metric,
  COUNT(*) as count
FROM silver.finance.fact_deals
WHERE amount_financed_amount = 0 
  AND payment_amount = 0 
  AND profit_amount = 0
UNION ALL
SELECT 
  'gold_existing_deals_without_financial_info' as metric,
  COUNT(*) as count
FROM gold.finance.fact_deals
WHERE amount_financed_amount = 0 
  AND payment_amount = 0 
  AND profit_amount = 0;

-- 11. Ally Fees Validation Across Layers
WITH bronze_ally_fees AS (
  SELECT 
    fi.option_type,
    COUNT(*) as deal_count,
    AVG(CASE 
      WHEN fi.option_type = 'vsc' THEN 675
      WHEN fi.option_type = 'gap' THEN 50
      WHEN fi.option_type = 'vscPlusGap' THEN 725
      ELSE 0
    END) as avg_ally_fees
  FROM bronze.leaseend_db_public.deals d
  JOIN bronze.leaseend_db_public.financial_infos fi ON d.id = fi.deal_id
  WHERE (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
    AND (fi._fivetran_deleted = FALSE OR fi._fivetran_deleted IS NULL)
    AND d.state IS NOT NULL 
    AND d.id != 0
  GROUP BY fi.option_type
)
SELECT 
  'bronze' as source,
  option_type,
  deal_count,
  avg_ally_fees,
  avg_ally_fees as min_ally_fees,
  avg_ally_fees as max_ally_fees
FROM bronze_ally_fees
UNION ALL
SELECT 
  'silver' as source,
  option_type_key as option_type,
  COUNT(*) as deal_count,
  AVG(ally_fees_amount) / 100.0 as avg_ally_fees,
  MIN(ally_fees_amount) / 100.0 as min_ally_fees,
  MAX(ally_fees_amount) / 100.0 as max_ally_fees
FROM silver.finance.fact_deals
GROUP BY option_type_key
UNION ALL
SELECT 
  'gold_existing' as source,
  option_type_key as option_type,
  COUNT(*) as deal_count,
  AVG(ally_fees_amount) / 100.0 as avg_ally_fees,
  MIN(ally_fees_amount) / 100.0 as min_ally_fees,
  MAX(ally_fees_amount) / 100.0 as max_ally_fees
FROM gold.finance.fact_deals
GROUP BY option_type_key
ORDER BY option_type, source;

-- 12. Schema Comparison - Show what fields are available in each layer
SELECT 
  'silver_fact_deals' as table_name,
  'deal_key' as column_name, 'STRING' as data_type, 'Primary key' as description
UNION ALL SELECT 'silver_fact_deals', 'amount_financed_amount', 'BIGINT', 'Amount financed in cents'
UNION ALL SELECT 'silver_fact_deals', 'profit_amount', 'BIGINT', 'Profit in cents'
UNION ALL SELECT 'silver_fact_deals', 'vsc_rev_amount', 'BIGINT', 'VSC revenue in cents'
UNION ALL SELECT 'silver_fact_deals', 'gap_rev_amount', 'BIGINT', 'GAP revenue in cents'
UNION ALL SELECT 'silver_fact_deals', 'rpt_amount', 'BIGINT', 'RPT amount in cents'
UNION ALL SELECT 'silver_fact_deals', '_source_file_name', 'STRING', 'Source: bronze tables'

UNION ALL SELECT 'gold_existing_fact_deals', 'deal_key', 'STRING', 'Primary key'
UNION ALL SELECT 'gold_existing_fact_deals', 'amount_financed_amount', 'BIGINT', 'Amount financed in cents'
UNION ALL SELECT 'gold_existing_fact_deals', 'profit_amount', 'BIGINT', 'Profit in cents'
UNION ALL SELECT 'gold_existing_fact_deals', 'vsc_rev_amount', 'BIGINT', 'VSC revenue in cents'
UNION ALL SELECT 'gold_existing_fact_deals', 'gap_rev_amount', 'BIGINT', 'GAP revenue in cents'
UNION ALL SELECT 'gold_existing_fact_deals', 'rpt_amount', 'BIGINT', 'RPT amount in cents'
UNION ALL SELECT 'gold_existing_fact_deals', '_source_file_name', 'STRING', 'Source: silver.deal.big_deal'

ORDER BY table_name, column_name; 