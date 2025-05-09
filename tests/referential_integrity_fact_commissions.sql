-- Referential Integrity Checks for fact_commissions

-- 1. Employee Key
SELECT COUNT(*) AS invalid_employee_keys
FROM gold.finance.fact_commissions f
LEFT JOIN gold.finance.dim_employee e ON f.employee_key = e.employee_key
WHERE e.employee_key IS NULL AND f.employee_key IS NOT NULL;

-- 2. Deal Key
SELECT COUNT(*) AS invalid_deal_keys
FROM gold.finance.fact_commissions f
LEFT JOIN gold.finance.fact_deals d ON f.deal_id = d.deal_id
WHERE d.deal_id IS NULL AND f.deal_id IS NOT NULL;

-- 3. Commission Date Key
SELECT COUNT(*) AS invalid_commission_date_keys
FROM gold.finance.fact_commissions f
LEFT JOIN gold.finance.dim_date d ON f.commission_date_key = d.date_key
WHERE d.date_key IS NULL AND f.commission_date_key IS NOT NULL; 