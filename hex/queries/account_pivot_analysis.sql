-- hex/queries/account_pivot_analysis.sql
-- Enhanced pivoting with the gold layer normalized fact table and account dimension

-- 1. Dynamic Pivot by Account Category with Enhanced Business Logic
-- This query leverages the gold layer enhancements for better business insights
SELECT 
    f.deal_key,
    f.vin,
    f.fiscal_year,
    f.fiscal_quarter,
    da.account_category,
    da.account_subcategory,
    SUM(CASE WHEN f.transaction_category = 'RESERVE' AND f.is_revenue THEN f.amount_dollars ELSE 0 END) as reserve_revenue,
    SUM(CASE WHEN f.transaction_category = 'VSC' AND f.is_revenue THEN f.amount_dollars ELSE 0 END) as vsc_revenue,
    SUM(CASE WHEN f.transaction_category = 'GAP' AND f.is_revenue THEN f.amount_dollars ELSE 0 END) as gap_revenue,
    SUM(CASE WHEN f.transaction_category = 'DOC_FEES' AND f.is_revenue THEN f.amount_dollars ELSE 0 END) as doc_fees_revenue,
    SUM(CASE WHEN f.is_revenue THEN f.amount_dollars ELSE 0 END) as total_revenue,
    SUM(CASE WHEN f.is_cost_of_revenue THEN f.amount_dollars ELSE 0 END) as cost_of_revenue,
    SUM(CASE WHEN f.is_operating_expense THEN f.amount_dollars ELSE 0 END) as operating_expenses,
    SUM(f.profit_contribution) as gross_profit,
    AVG(f.data_quality_score) as avg_data_quality_score
FROM gold.finance.fact_deal_netsuite_transactions f
INNER JOIN gold.finance.dim_account da ON f.account_key = da.account_key
WHERE f.fiscal_year = {{ year_filter }}
    AND f.fiscal_quarter = {{ quarter_filter }}
    AND da.account_category IN {{ account_categories }}
GROUP BY f.deal_key, f.vin, f.fiscal_year, f.fiscal_quarter, da.account_category, da.account_subcategory
ORDER BY gross_profit DESC;

-- 2. Account Hierarchy Analysis with Business Groupings
-- Analyze performance using account hierarchy and enhanced business classifications
SELECT 
    da.account_category,
    da.account_subcategory,
    parent_da.account_display_name as parent_account,
    da.account_display_name as account_name,
    f.transaction_group,
    f.transaction_type,
    f.transaction_category,
    COUNT(DISTINCT f.deal_key) as deal_count,
    SUM(f.amount_dollars) as total_amount,
    SUM(f.profit_contribution) as profit_contribution,
    AVG(f.amount_dollars) as avg_amount_per_transaction,
    AVG(f.data_quality_score) as avg_data_quality,
    SUM(CASE WHEN f.allocation_method = 'VIN_MATCH' THEN f.amount_dollars ELSE 0 END) as vin_matched_amount,
    SUM(CASE WHEN f.allocation_method = 'PERIOD_ALLOCATION' THEN f.amount_dollars ELSE 0 END) as allocated_amount
FROM gold.finance.fact_deal_netsuite_transactions f
INNER JOIN gold.finance.dim_account da ON f.account_key = da.account_key
LEFT JOIN gold.finance.dim_account parent_da ON da.parent_account_id = parent_da.account_id
WHERE f.fiscal_year >= {{ start_year }}
    AND f.fiscal_year <= {{ end_year }}
GROUP BY 
    da.account_category, da.account_subcategory, parent_da.account_display_name,
    da.account_display_name, f.transaction_group, f.transaction_type, f.transaction_category
ORDER BY profit_contribution DESC;

-- 3. Enhanced P&L Analysis by Transaction Group
-- Leverage the enhanced business groupings for P&L analysis
SELECT 
    f.fiscal_year,
    f.fiscal_quarter,
    f.transaction_group,
    da.account_category,
    SUM(CASE WHEN f.transaction_group = 'PRODUCT_REVENUE' THEN f.amount_dollars ELSE 0 END) as product_revenue,
    SUM(CASE WHEN f.transaction_group = 'COST_OF_REVENUE' THEN f.amount_dollars ELSE 0 END) as cost_of_revenue,
    SUM(CASE WHEN f.transaction_group = 'OPERATING_EXPENSE' THEN f.amount_dollars ELSE 0 END) as operating_expenses,
    SUM(CASE WHEN f.transaction_group = 'PRODUCT_REVENUE' THEN f.amount_dollars ELSE 0 END) - 
    SUM(CASE WHEN f.transaction_group = 'COST_OF_REVENUE' THEN f.amount_dollars ELSE 0 END) as gross_profit,
    SUM(f.profit_contribution) as net_profit,
    COUNT(DISTINCT f.deal_key) as deal_count,
    SUM(f.profit_contribution) / NULLIF(COUNT(DISTINCT f.deal_key), 0) as profit_per_deal,
    AVG(f.data_quality_score) as avg_data_quality_score
FROM gold.finance.fact_deal_netsuite_transactions f
INNER JOIN gold.finance.dim_account da ON f.account_key = da.account_key
WHERE f.fiscal_year = {{ year_filter }}
    AND da.is_inactive = FALSE
GROUP BY f.fiscal_year, f.fiscal_quarter, f.transaction_group, da.account_category
ORDER BY f.fiscal_quarter, net_profit DESC;

-- 4. Data Quality and Allocation Analysis
-- Understand data quality and allocation methods impact
SELECT 
    f.allocation_method,
    f.transaction_group,
    da.account_category,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT f.deal_key) as deal_count,
    SUM(f.amount_dollars) as total_amount,
    AVG(f.data_quality_score) as avg_data_quality_score,
    MIN(f.data_quality_score) as min_data_quality_score,
    MAX(f.data_quality_score) as max_data_quality_score,
    SUM(f.amount_dollars) / NULLIF(COUNT(DISTINCT f.deal_key), 0) as amount_per_deal,
    STDDEV(f.amount_dollars) as amount_std_dev
FROM gold.finance.fact_deal_netsuite_transactions f
INNER JOIN gold.finance.dim_account da ON f.account_key = da.account_key
WHERE f.fiscal_year = {{ year_filter }}
GROUP BY f.allocation_method, f.transaction_group, da.account_category
ORDER BY f.allocation_method, total_amount DESC;

-- 5. Account Performance Trends with Enhanced Metrics
-- Track account performance over time with business enhancements
SELECT 
    f.fiscal_year,
    f.fiscal_quarter,
    da.account_display_name,
    da.account_category,
    da.account_subcategory,
    f.transaction_group,
    SUM(f.amount_dollars) as total_amount,
    SUM(f.profit_contribution) as profit_contribution,
    COUNT(DISTINCT f.deal_key) as deal_count,
    AVG(f.revenue_per_deal) as avg_revenue_per_deal,
    AVG(f.expense_per_deal) as avg_expense_per_deal,
    AVG(f.data_quality_score) as avg_data_quality,
    SUM(f.amount_dollars) / NULLIF(COUNT(DISTINCT f.deal_key), 0) as amount_per_deal,
    -- Calculate quarter-over-quarter growth
    LAG(SUM(f.amount_dollars)) OVER (
        PARTITION BY da.account_key, f.transaction_group 
        ORDER BY f.fiscal_year, f.fiscal_quarter
    ) as previous_quarter_amount,
    (SUM(f.amount_dollars) - LAG(SUM(f.amount_dollars)) OVER (
        PARTITION BY da.account_key, f.transaction_group 
        ORDER BY f.fiscal_year, f.fiscal_quarter
    )) / NULLIF(LAG(SUM(f.amount_dollars)) OVER (
        PARTITION BY da.account_key, f.transaction_group 
        ORDER BY f.fiscal_year, f.fiscal_quarter
    ), 0) * 100 as qoq_growth_percent
FROM gold.finance.fact_deal_netsuite_transactions f
INNER JOIN gold.finance.dim_account da ON f.account_key = da.account_key
WHERE f.fiscal_year >= {{ start_year }}
    AND da.account_category IN {{ account_categories }}
GROUP BY 
    f.fiscal_year, f.fiscal_quarter, da.account_display_name, da.account_category, 
    da.account_subcategory, f.transaction_group, da.account_key
ORDER BY f.fiscal_year, f.fiscal_quarter, profit_contribution DESC;

-- 6. Deal-Level Enhanced P&L Statement
-- Create a comprehensive P&L for a specific deal using enhanced classifications
SELECT 
    da.account_category,
    da.account_subcategory,
    f.transaction_group,
    f.transaction_category,
    f.transaction_subcategory,
    da.account_display_name,
    f.amount_dollars,
    f.profit_contribution,
    f.allocation_method,
    f.data_quality_score,
    CASE 
        WHEN f.transaction_group = 'PRODUCT_REVENUE' THEN 1
        WHEN f.transaction_group = 'COST_OF_REVENUE' THEN 2
        WHEN f.transaction_group = 'OPERATING_EXPENSE' THEN 3
        ELSE 4
    END as sort_order
FROM gold.finance.fact_deal_netsuite_transactions f
INNER JOIN gold.finance.dim_account da ON f.account_key = da.account_key
WHERE f.deal_key = {{ deal_id }}
ORDER BY sort_order, da.account_category, f.transaction_category, da.account_display_name;

-- 7. Account Balance Sheet vs Income Statement Analysis
-- Leverage account dimension classifications for financial statement analysis
SELECT 
    CASE 
        WHEN da.is_balance_sheet THEN 'Balance Sheet'
        WHEN da.is_income_statement THEN 'Income Statement'
        ELSE 'Other'
    END as financial_statement_type,
    da.account_category,
    f.transaction_group,
    f.fiscal_year,
    f.fiscal_quarter,
    COUNT(DISTINCT f.deal_key) as deal_count,
    SUM(f.amount_dollars) as total_amount,
    SUM(f.profit_contribution) as profit_contribution,
    AVG(f.data_quality_score) as avg_data_quality,
    SUM(CASE WHEN f.allocation_method = 'VIN_MATCH' THEN 1 ELSE 0 END) as vin_matched_transactions,
    COUNT(*) as total_transactions
FROM gold.finance.fact_deal_netsuite_transactions f
INNER JOIN gold.finance.dim_account da ON f.account_key = da.account_key
WHERE f.fiscal_year = {{ year_filter }}
    AND (da.is_balance_sheet = TRUE OR da.is_income_statement = TRUE)
GROUP BY 
    da.is_balance_sheet, da.is_income_statement, da.account_category, 
    f.transaction_group, f.fiscal_year, f.fiscal_quarter
ORDER BY financial_statement_type, da.account_category, f.transaction_group;

-- 8. Enhanced Account Performance Comparison with Business Context
-- Compare account performance with enhanced business context
WITH current_period AS (
    SELECT 
        f.account_key,
        da.account_display_name,
        da.account_category,
        f.transaction_group,
        SUM(f.amount_dollars) as current_amount,
        SUM(f.profit_contribution) as current_profit,
        COUNT(DISTINCT f.deal_key) as current_deals,
        AVG(f.data_quality_score) as current_data_quality
    FROM gold.finance.fact_deal_netsuite_transactions f
    INNER JOIN gold.finance.dim_account da ON f.account_key = da.account_key
    WHERE f.fiscal_year = {{ current_year }} AND f.fiscal_quarter = {{ current_quarter }}
    GROUP BY f.account_key, da.account_display_name, da.account_category, f.transaction_group
),
previous_period AS (
    SELECT 
        f.account_key,
        f.transaction_group,
        SUM(f.amount_dollars) as previous_amount,
        SUM(f.profit_contribution) as previous_profit,
        COUNT(DISTINCT f.deal_key) as previous_deals,
        AVG(f.data_quality_score) as previous_data_quality
    FROM gold.finance.fact_deal_netsuite_transactions f
    WHERE f.fiscal_year = {{ previous_year }} AND f.fiscal_quarter = {{ previous_quarter }}
    GROUP BY f.account_key, f.transaction_group
)
SELECT 
    cp.account_display_name,
    cp.account_category,
    cp.transaction_group,
    cp.current_amount,
    pp.previous_amount,
    cp.current_amount - COALESCE(pp.previous_amount, 0) as amount_change,
    CASE 
        WHEN pp.previous_amount > 0 THEN 
            ((cp.current_amount - pp.previous_amount) / pp.previous_amount) * 100
        ELSE NULL 
    END as percent_change,
    cp.current_profit,
    pp.previous_profit,
    cp.current_profit - COALESCE(pp.previous_profit, 0) as profit_change,
    cp.current_deals,
    pp.previous_deals,
    cp.current_data_quality,
    pp.previous_data_quality
FROM current_period cp
FULL OUTER JOIN previous_period pp ON cp.account_key = pp.account_key AND cp.transaction_group = pp.transaction_group
ORDER BY ABS(cp.current_amount - COALESCE(pp.previous_amount, 0)) DESC; 