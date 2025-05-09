-- Additional Data Quality Checks

-- 1. Dimension Key Completeness (fact_deals example)
SELECT COUNT(*) AS deals_with_null_driver_key
FROM gold.finance.fact_deals
WHERE driver_key IS NULL;

-- 2. Duplicate Primary Keys (dim_driver example)
SELECT driver_key, COUNT(*) AS cnt
FROM gold.finance.dim_driver
GROUP BY driver_key
HAVING cnt > 1;

-- 3. Date Range Validity (fact_deals and dim_date)
SELECT MIN(creation_date_key) AS min_fact_date, MAX(creation_date_key) AS max_fact_date
FROM gold.finance.fact_deals;

SELECT MIN(date_key) AS min_dim_date, MAX(date_key) AS max_dim_date
FROM gold.finance.dim_date;

-- 4. Financial Metric Sanity Checks (fact_deals example)
SELECT COUNT(*) AS negative_amount_financed
FROM gold.finance.fact_deals
WHERE amount_financed < 0; 