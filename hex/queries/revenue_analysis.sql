-- SQL Template for Revenue Analysis in Hex
-- Use this query in a Hex SQL cell.
-- Assumes Hex Input Parameters named 'date_range' (Date range type) and 'product_types' (Multiselect type) exist.
-- Assumes tables are in finance_dw.facts and finance_dw.dimensions catalogs.
-- Adjust column names (e.g., d.year, p.product_type) based on final dimension table definitions.

SELECT
  d.year, -- Adjust if using different year column name in dim_date
  d.month, -- Adjust if using different month column name in dim_date
  p.product_type, -- Adjust if using different product type column name in dim_product
  SUM(f.vsc_rev) AS vsc_revenue,
  SUM(f.gap_rev) AS gap_revenue,
  SUM(f.reserve) AS reserve_revenue,
  SUM(f.profit) AS total_profit
FROM finance_dw.facts.fact_deals f
JOIN finance_dw.dimensions.dim_date d ON f.creation_date_key = d.date_key
JOIN finance_dw.dimensions.dim_product p ON f.product_key = p.product_key
WHERE d.date BETWEEN '{{ date_range.start }}' AND '{{ date_range.end }}'
  AND p.product_type IN ( {{ product_types }} ) -- Assumes product_types returns a comma-separated list suitable for IN clause
GROUP BY d.year, d.month, p.product_type
ORDER BY d.year, d.month, p.product_type; 