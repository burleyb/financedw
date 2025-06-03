-- hex/queries/account_analysis.sql
-- Sample queries for NetSuite account analysis using the new dim_account table

-- 1. Account Summary by Category
-- This query provides a high-level overview of accounts by category
SELECT 
    account_category,
    account_subcategory,
    COUNT(*) as account_count,
    COUNT(CASE WHEN is_inactive = FALSE THEN 1 END) as active_accounts,
    COUNT(CASE WHEN is_summary = TRUE THEN 1 END) as summary_accounts,
    SUM(balance) as total_balance,
    SUM(available_balance) as total_available_balance
FROM gold.finance.dim_account
WHERE account_key NOT IN ('No Account', 'Unknown')
GROUP BY account_category, account_subcategory
ORDER BY account_category, account_subcategory;

-- 2. Balance Sheet Accounts
-- Query for balance sheet reporting
SELECT 
    account_category,
    account_subcategory,
    account_display_name,
    balance,
    available_balance,
    is_summary,
    is_inactive
FROM gold.finance.dim_account
WHERE is_balance_sheet = TRUE
    AND is_inactive = FALSE
    AND account_key NOT IN ('No Account', 'Unknown')
ORDER BY 
    CASE account_category 
        WHEN 'Assets' THEN 1 
        WHEN 'Liabilities' THEN 2 
        WHEN 'Equity' THEN 3 
        ELSE 4 
    END,
    account_subcategory,
    account_display_name;

-- 3. Income Statement Accounts
-- Query for P&L reporting
SELECT 
    account_category,
    account_subcategory,
    account_display_name,
    balance,
    is_summary,
    is_inactive
FROM gold.finance.dim_account
WHERE is_income_statement = TRUE
    AND is_inactive = FALSE
    AND account_key NOT IN ('No Account', 'Unknown')
ORDER BY 
    CASE account_category 
        WHEN 'Revenue' THEN 1 
        WHEN 'Expenses' THEN 2 
        ELSE 3 
    END,
    account_subcategory,
    account_display_name;

-- 4. Bank Accounts Summary
-- Query for cash management
SELECT 
    account_display_name,
    bank_name,
    bank_routing_number,
    balance,
    available_balance,
    subsidiary,
    is_inactive
FROM gold.finance.dim_account
WHERE is_bank_account = TRUE
    AND account_key NOT IN ('No Account', 'Unknown')
ORDER BY bank_name, account_display_name;

-- 5. Account Hierarchy Analysis
-- Shows parent-child relationships (simplified)
SELECT 
    parent.account_display_name as parent_account,
    child.account_display_name as child_account,
    child.account_category,
    child.account_subcategory,
    child.balance,
    child.is_summary as child_is_summary
FROM gold.finance.dim_account child
LEFT JOIN gold.finance.dim_account parent 
    ON child.parent_account_id = parent.account_id
WHERE child.parent_account_id IS NOT NULL
    AND child.account_key NOT IN ('No Account', 'Unknown')
    AND child.is_inactive = FALSE
ORDER BY parent.account_display_name, child.account_display_name;

-- 6. Accounts with Zero Balance
-- Useful for account cleanup analysis
SELECT 
    account_display_name,
    account_category,
    account_subcategory,
    balance,
    available_balance,
    is_inactive,
    description
FROM gold.finance.dim_account
WHERE (balance = 0 OR balance IS NULL)
    AND (available_balance = 0 OR available_balance IS NULL)
    AND account_key NOT IN ('No Account', 'Unknown')
    AND is_summary = FALSE
ORDER BY account_category, account_subcategory, account_display_name;

-- 7. Account Activity Summary for Dashboards
-- Aggregated view suitable for executive dashboards
SELECT 
    'Total Assets' as metric_name,
    SUM(balance) as amount
FROM gold.finance.dim_account
WHERE account_category = 'Assets' 
    AND is_inactive = FALSE
    AND account_key NOT IN ('No Account', 'Unknown')

UNION ALL

SELECT 
    'Total Liabilities' as metric_name,
    SUM(balance) as amount
FROM gold.finance.dim_account
WHERE account_category = 'Liabilities' 
    AND is_inactive = FALSE
    AND account_key NOT IN ('No Account', 'Unknown')

UNION ALL

SELECT 
    'Total Equity' as metric_name,
    SUM(balance) as amount
FROM gold.finance.dim_account
WHERE account_category = 'Equity' 
    AND is_inactive = FALSE
    AND account_key NOT IN ('No Account', 'Unknown')

UNION ALL

SELECT 
    'Total Revenue' as metric_name,
    SUM(balance) as amount
FROM gold.finance.dim_account
WHERE account_category = 'Revenue' 
    AND is_inactive = FALSE
    AND account_key NOT IN ('No Account', 'Unknown')

UNION ALL

SELECT 
    'Total Expenses' as metric_name,
    SUM(balance) as amount
FROM gold.finance.dim_account
WHERE account_category = 'Expenses' 
    AND is_inactive = FALSE
    AND account_key NOT IN ('No Account', 'Unknown')

ORDER BY 
    CASE metric_name 
        WHEN 'Total Assets' THEN 1
        WHEN 'Total Liabilities' THEN 2
        WHEN 'Total Equity' THEN 3
        WHEN 'Total Revenue' THEN 4
        WHEN 'Total Expenses' THEN 5
    END; 