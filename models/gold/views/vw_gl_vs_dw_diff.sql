-- models/gold/views/vw_gl_vs_dw_diff.sql
-- Enhanced finance reconciliation view: compares Delta warehouse fact totals with both posted and total NetSuite amounts
-- Shows posted vs unposted transaction differences for better operational visibility
-- Usage: SELECT * FROM gold.finance.vw_gl_vs_dw_diff WHERE year = 2025 AND month = 2 ORDER BY ABS(total_difference) DESC;

CREATE OR REPLACE VIEW gold.finance.vw_gl_vs_dw_diff AS
WITH dw AS (
    SELECT
        da.account_number,
        da.account_name,
        dt.year AS year,
        dt.month AS month,
        SUM(f.amount_dollars) AS dw_total
    FROM silver.finance.fact_deal_netsuite_transactions f
    JOIN silver.finance.dim_account da ON f.account_key = da.account_key
    JOIN silver.finance.dim_date dt ON f.revenue_recognition_date_key = dt.date_key
    WHERE CAST(REGEXP_EXTRACT(da.account_number, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
    GROUP BY da.account_number, da.account_name, dt.year, dt.month
),
-- Posted NetSuite GL amounts (from transactionaccountingline - official GL postings)
ns_posted AS (
    SELECT
        a.acctnumber  AS account_number,
        a.fullname    AS account_name,
        YEAR(t.trandate)  AS year,
        MONTH(t.trandate) AS month,
        SUM(
            CASE
                WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999
                     THEN tal.amount * -1      -- flip sign for revenue accounts to be positive
                ELSE tal.amount * -1           -- flip sign for expense accounts to be negative
            END
        ) AS posted_total
    FROM bronze.ns.transactionaccountingline tal
    JOIN bronze.ns.transaction t ON tal.transaction = t.id
    JOIN bronze.ns.account a ON tal.account = a.id
    WHERE tal.posting = 'T'
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
    GROUP BY a.acctnumber, a.fullname, YEAR(t.trandate), MONTH(t.trandate)
),

-- Total NetSuite amounts (includes unposted transactions - from both revenue and expense sources)
ns_total AS (
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
            SUM(si.amount) AS total_amount  -- Revenue amounts are already positive in salesinvoiced
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
            SUM(tl.foreignamount) AS total_amount  -- flip sign for expense accounts to be negative
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
    COALESCE(dw.account_number, ns_posted.account_number, ns_total.account_number) AS account_number,
    COALESCE(dw.account_name, ns_posted.account_name, ns_total.account_name) AS account_name,
    COALESCE(dw.year, ns_posted.year, ns_total.year) AS year,
    COALESCE(dw.month, ns_posted.month, ns_total.month) AS month,
    
    -- Data warehouse amount (what we're using for reporting)
    COALESCE(dw.dw_total, 0) AS dw_total,
    
    -- NetSuite posted amount (official GL - used in financial statements)
    COALESCE(ns_posted.posted_total, 0) AS ns_posted_total,
    
    -- NetSuite total amount (includes unposted transactions - matches DW source)
    COALESCE(ns_total.total_amount, 0) AS ns_total_amount,
    
    -- Unposted amount (difference between total and posted)
    COALESCE(ns_total.total_amount, 0) - COALESCE(ns_posted.posted_total, 0) AS ns_unposted_amount,
    
    -- Posted difference (DW vs official GL)
    COALESCE(dw.dw_total, 0) - COALESCE(ns_posted.posted_total, 0) AS posted_difference,
    
    -- Total difference (DW vs all NetSuite transactions)
    COALESCE(dw.dw_total, 0) - COALESCE(ns_total.total_amount, 0) AS total_difference,
    
    -- Status flags for easy filtering
    CASE 
        WHEN ABS(COALESCE(dw.dw_total, 0) - COALESCE(ns_posted.posted_total, 0)) < 0.01 THEN 'POSTED_MATCH'
        WHEN ABS(COALESCE(dw.dw_total, 0) - COALESCE(ns_total.total_amount, 0)) < 0.01 THEN 'TOTAL_MATCH'
        WHEN ABS(COALESCE(ns_total.total_amount, 0) - COALESCE(ns_posted.posted_total, 0)) > 0.01 THEN 'HAS_UNPOSTED'
        ELSE 'MISMATCH'
    END AS reconciliation_status

FROM dw
FULL OUTER JOIN ns_posted
  ON dw.account_number = ns_posted.account_number
 AND dw.year = ns_posted.year
 AND dw.month = ns_posted.month
FULL OUTER JOIN ns_total
  ON COALESCE(dw.account_number, ns_posted.account_number) = ns_total.account_number
 AND COALESCE(dw.year, ns_posted.year) = ns_total.year
 AND COALESCE(dw.month, ns_posted.month) = ns_total.month; 