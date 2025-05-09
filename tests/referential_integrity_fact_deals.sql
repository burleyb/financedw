-- Referential Integrity Checks for fact_deals

-- 1. Driver Key
SELECT COUNT(*) AS invalid_driver_keys
FROM gold.finance.fact_deals f
LEFT JOIN gold.finance.dim_driver d ON f.driver_key = d.driver_key
WHERE d.driver_key IS NULL AND f.driver_key IS NOT NULL;

-- 2. Vehicle Key
SELECT COUNT(*) AS invalid_vehicle_keys
FROM gold.finance.fact_deals f
LEFT JOIN gold.finance.dim_vehicle v ON f.vehicle_key = v.vehicle_key
WHERE v.vehicle_key IS NULL AND f.vehicle_key IS NOT NULL;

-- 3. Employee Key
SELECT COUNT(*) AS invalid_employee_keys
FROM gold.finance.fact_deals f
LEFT JOIN gold.finance.dim_employee e ON f.employee_key = e.employee_key
WHERE e.employee_key IS NULL AND f.employee_key IS NOT NULL;

-- 4. Creation Date Key
SELECT COUNT(*) AS invalid_creation_date_keys
FROM gold.finance.fact_deals f
LEFT JOIN gold.finance.dim_date d ON f.creation_date_key = d.date_key
WHERE d.date_key IS NULL AND f.creation_date_key IS NOT NULL;

-- 5. Completion Date Key
SELECT COUNT(*) AS invalid_completion_date_keys
FROM gold.finance.fact_deals f
LEFT JOIN gold.finance.dim_date d ON f.completion_date_key = d.date_key
WHERE d.date_key IS NULL AND f.completion_date_key IS NOT NULL;

-- 6. Deal Status Key
SELECT COUNT(*) AS invalid_deal_status_keys
FROM gold.finance.fact_deals f
LEFT JOIN gold.finance.dim_deal_status ds ON f.deal_status_key = ds.deal_status_key
WHERE ds.deal_status_key IS NULL AND f.deal_status_key IS NOT NULL;

-- 7. Product Key
SELECT COUNT(*) AS invalid_product_keys
FROM gold.finance.fact_deals f
LEFT JOIN gold.finance.dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL AND f.product_key IS NOT NULL; 