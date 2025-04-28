-- Performance Tuning Examples for Finance DW
-- Apply these techniques based on observed query patterns and performance bottlenecks.

-- 1. Clustering (Example: Cluster fact_deals on common filter columns)
-- Choose columns frequently used in WHERE clauses or JOIN conditions.
-- Clustering physically co-locates data with similar values in these columns.
-- Note: Clustering is beneficial for large tables (TBs).

-- ALTER TABLE finance_dw.facts.fact_deals CLUSTER BY (creation_date_key, product_key);

-- 2. Z-Ordering (Example: Z-Order fact_deals on multiple filter columns)
-- Z-Ordering interleaves data across multiple dimensions, improving data skipping.
-- Effective when filtering on multiple columns independently.
-- Apply Z-ORDER after initial data load and periodically.

-- OPTIMIZE finance_dw.facts.fact_deals ZORDER BY (creation_date_key, employee_key, deal_status_key);

-- 3. Aggregate Tables (Mart Tables)
-- Pre-compute summaries for frequently requested aggregations.
-- Reduces query complexity and improves dashboard loading times.
-- Example: Monthly revenue summary (from implementation_plan.mdc)

/* -- Create or replace this table in the finance_dw.mart schema
CREATE OR REPLACE TABLE finance_dw.mart.monthly_revenue_summary
COMMENT 'Pre-aggregated monthly revenue and profit summary by product and deal state.'
USING DELTA
AS SELECT
   d.year, -- Adjust based on dim_date
   d.month, -- Adjust based on dim_date
   p.product_type, -- Adjust based on dim_product
   ds.deal_state, -- Adjust based on dim_deal_status
   COUNT(*) AS deal_count,
   SUM(f.profit) AS total_profit,
   SUM(f.vsc_rev) AS total_vsc_revenue,
   SUM(f.gap_rev) AS total_gap_revenue
FROM finance_dw.facts.fact_deals f
JOIN finance_dw.dimensions.dim_date d ON f.creation_date_key = d.date_key
JOIN finance_dw.dimensions.dim_product p ON f.product_key = p.product_key
JOIN finance_dw.dimensions.dim_deal_status ds ON f.deal_status_key = ds.deal_status_key
GROUP BY d.year, d.month, p.product_type, ds.deal_state;
*/

-- Regularly refresh mart tables as part of the ETL workflow (e.g., daily/hourly).

-- 4. Analyze Tables (Optional but recommended after major changes)
-- Collects statistics used by the query optimizer.

-- ANALYZE TABLE finance_dw.facts.fact_deals COMPUTE STATISTICS FOR ALL COLUMNS;
-- ANALYZE TABLE finance_dw.dimensions.dim_date COMPUTE STATISTICS FOR ALL COLUMNS;
-- ... analyze other dimension tables ... 