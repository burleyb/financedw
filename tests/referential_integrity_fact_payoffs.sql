-- Referential Integrity Checks for fact_payoffs

-- 1. Deal Key
SELECT COUNT(*) AS invalid_deal_keys
FROM gold.finance.fact_payoffs f
LEFT JOIN gold.finance.fact_deals d ON f.deal_id = d.deal_id
WHERE d.deal_id IS NULL AND f.deal_id IS NOT NULL;

-- 2. Payoff Date Key
SELECT COUNT(*) AS invalid_payoff_date_keys
FROM gold.finance.fact_payoffs f
LEFT JOIN gold.finance.dim_date d ON f.payoff_date_key = d.date_key
WHERE d.date_key IS NULL AND f.payoff_date_key IS NOT NULL; 