-- models/gold/views/vw_gl_vs_dw_full_diff.sql
-- Enhanced full reconciliation: compares entire NetSuite GL with data-warehouse facts
-- Shows posted vs total amounts for P&L accounts, includes Balance-Sheet facts
-- Provides visibility into unposted P&L transactions while maintaining full GL coverage

CREATE OR REPLACE VIEW gold.finance.vw_gl_vs_dw_full_diff AS
WITH dw AS (
    SELECT account_number, account_name, year, month, SUM(total) AS dw_total
    FROM (
        -- P&L facts
        SELECT da.account_number, da.account_name, dt.year AS year, dt.month AS month,
               SUM(f.amount_dollars) AS total
        FROM silver.finance.fact_deal_netsuite_transactions f
        JOIN silver.finance.dim_account da ON f.account_key = da.account_key
        JOIN silver.finance.dim_date    dt ON f.revenue_recognition_date_key = dt.date_key
        GROUP BY da.account_number, da.account_name, dt.year, dt.month
        UNION ALL
        -- Balance-sheet facts
        SELECT da.account_number, da.account_name, f.year, f.month,
               SUM(f.amount_dollars) AS total
        FROM silver.finance.fact_gl_balance_sheet f
        JOIN silver.finance.dim_account da ON f.account_key = da.account_key
        GROUP BY da.account_number, da.account_name, f.year, f.month
    ) sub
    GROUP BY account_number, account_name, year, month
),
-- Posted NetSuite GL totals (official GL postings for all accounts)
ns_posted AS (
    SELECT a.acctnumber AS account_number,
           a.fullname   AS account_name,
           YEAR(t.trandate)  AS year,
           MONTH(t.trandate) AS month,
           SUM(tal.amount)   AS posted_total
    FROM bronze.ns.transactionaccountingline tal
    JOIN bronze.ns.transaction t ON tal.transaction = t.id
    JOIN bronze.ns.account      a ON tal.account     = a.id
    WHERE tal.posting='T'
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND (t._fivetran_deleted   = FALSE OR t._fivetran_deleted   IS NULL)
    GROUP BY a.acctnumber, a.fullname, YEAR(t.trandate), MONTH(t.trandate)
),
-- Total NetSuite amounts for P&L accounts only (includes unposted transactions)
-- Balance sheet accounts typically don't have the same unposted transaction issues
ns_total_pl AS (
    SELECT
        account_number,
        account_name,
        year,
        month,
        SUM(total_amount) AS total_amount
    FROM (
        -- Revenue accounts from salesinvoiced
        SELECT
            a.acctnumber AS account_number,
            a.fullname AS account_name,
            YEAR(t.trandate) AS year,
            MONTH(t.trandate) AS month,
            SUM(si.amount) AS total_amount
        FROM bronze.ns.salesinvoiced si
        JOIN bronze.ns.transaction t ON si.transaction = t.id
        JOIN bronze.ns.account a ON si.account = a.id
        WHERE (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999
        GROUP BY a.acctnumber, a.fullname, YEAR(t.trandate), MONTH(t.trandate)
        
        UNION ALL
        
        -- Expense accounts from transactionline  
        SELECT
            a.acctnumber AS account_number,
            a.fullname AS account_name,
            YEAR(t.trandate) AS year,
            MONTH(t.trandate) AS month,
            SUM(tl.foreignamount) AS total_amount  -- Keep original signs for consistency
        FROM bronze.ns.transactionline tl
        JOIN bronze.ns.transaction t ON tl.transaction = t.id
        JOIN bronze.ns.account a ON tl.expenseaccount = a.id
        WHERE (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND (tl._fivetran_deleted = FALSE OR tl._fivetran_deleted IS NULL)
          AND CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 5000 AND 8999
        GROUP BY a.acctnumber, a.fullname, YEAR(t.trandate), MONTH(t.trandate)
    ) combined
    GROUP BY account_number, account_name, year, month
)
SELECT 
    COALESCE(dw.account_number, ns_posted.account_number, ns_total_pl.account_number) AS account_number,
    COALESCE(dw.account_name, ns_posted.account_name, ns_total_pl.account_name) AS account_name,
    COALESCE(dw.year, ns_posted.year, ns_total_pl.year) AS year,
    COALESCE(dw.month, ns_posted.month, ns_total_pl.month) AS month,
    
    -- Data warehouse total (P&L + Balance Sheet)
    COALESCE(dw.dw_total, 0) AS dw_total,
    
    -- NetSuite posted total (official GL for all accounts)
    COALESCE(ns_posted.posted_total, 0) AS ns_posted_total,
    
    -- NetSuite total amount (includes unposted for P&L accounts only)
    CASE 
        WHEN CAST(REGEXP_EXTRACT(COALESCE(dw.account_number, ns_posted.account_number, ns_total_pl.account_number), '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999 
        THEN COALESCE(ns_total_pl.total_amount, ns_posted.posted_total, 0)
        ELSE COALESCE(ns_posted.posted_total, 0)  -- For balance sheet accounts, use posted amount
    END AS ns_total_amount,
    
    -- Unposted amount (only for P&L accounts)
    CASE 
        WHEN CAST(REGEXP_EXTRACT(COALESCE(dw.account_number, ns_posted.account_number, ns_total_pl.account_number), '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999 
        THEN COALESCE(ns_total_pl.total_amount, 0) - COALESCE(ns_posted.posted_total, 0)
        ELSE 0  -- Balance sheet accounts don't have unposted amounts in this analysis
    END AS ns_unposted_amount,
    
    -- Posted difference (DW vs official GL)
    COALESCE(dw.dw_total, 0) - COALESCE(ns_posted.posted_total, 0) AS posted_difference,
    
    -- Total difference (DW vs all NetSuite transactions)
    COALESCE(dw.dw_total, 0) - 
    CASE 
        WHEN CAST(REGEXP_EXTRACT(COALESCE(dw.account_number, ns_posted.account_number, ns_total_pl.account_number), '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999 
        THEN COALESCE(ns_total_pl.total_amount, ns_posted.posted_total, 0)
        ELSE COALESCE(ns_posted.posted_total, 0)
    END AS total_difference,
    
    -- Account type classification
    CASE 
        WHEN CAST(REGEXP_EXTRACT(COALESCE(dw.account_number, ns_posted.account_number, ns_total_pl.account_number), '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999 THEN 'P&L'
        ELSE 'Balance Sheet'
    END AS account_type,
    
    -- Enhanced status flags
    CASE 
        WHEN ABS(COALESCE(dw.dw_total, 0) - COALESCE(ns_posted.posted_total, 0)) < 0.01 THEN 'POSTED_MATCH'
        WHEN CAST(REGEXP_EXTRACT(COALESCE(dw.account_number, ns_posted.account_number, ns_total_pl.account_number), '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999 
             AND ABS(COALESCE(dw.dw_total, 0) - COALESCE(ns_total_pl.total_amount, ns_posted.posted_total, 0)) < 0.01 THEN 'TOTAL_MATCH'
        WHEN CAST(REGEXP_EXTRACT(COALESCE(dw.account_number, ns_posted.account_number, ns_total_pl.account_number), '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999 
             AND ABS(COALESCE(ns_total_pl.total_amount, 0) - COALESCE(ns_posted.posted_total, 0)) > 0.01 THEN 'HAS_UNPOSTED'
        ELSE 'MISMATCH'
    END AS reconciliation_status

FROM dw
FULL OUTER JOIN ns_posted
  ON dw.account_number = ns_posted.account_number
 AND dw.year = ns_posted.year
 AND dw.month = ns_posted.month
FULL OUTER JOIN ns_total_pl
  ON COALESCE(dw.account_number, ns_posted.account_number) = ns_total_pl.account_number
 AND COALESCE(dw.year, ns_posted.year) = ns_total_pl.year
 AND COALESCE(dw.month, ns_posted.month) = ns_total_pl.month; 