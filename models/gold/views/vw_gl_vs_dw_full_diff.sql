-- models/gold/views/vw_gl_vs_dw_full_diff.sql
-- Reconciles entire NetSuite GL (all accounts) with data-warehouse facts
-- Combines P&L fact (fact_deal_netsuite_transactions) and Balance-Sheet fact (fact_gl_balance_sheet)

CREATE OR REPLACE VIEW gold.finance.vw_gl_vs_dw_full_diff AS
-- 1. Warehouse totals (P&L + Balance-Sheet)
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
-- 2. Raw GL totals (posting lines)
gl AS (
    SELECT a.acctnumber AS account_number,
           a.fullname   AS account_name,
           YEAR(t.trandate)  AS year,
           MONTH(t.trandate) AS month,
           SUM(tal.amount)   AS gl_total
    FROM bronze.ns.transactionaccountingline tal
    JOIN bronze.ns.transaction t ON tal.transaction = t.id
    JOIN bronze.ns.account      a ON tal.account     = a.id
    WHERE tal.posting='T'
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND (t._fivetran_deleted   = FALSE OR t._fivetran_deleted   IS NULL)
    GROUP BY a.acctnumber, a.fullname, YEAR(t.trandate), MONTH(t.trandate)
)
SELECT COALESCE(dw.account_number, gl.account_number) AS account_number,
       COALESCE(dw.account_name,   gl.account_name)   AS account_name,
       COALESCE(dw.year,           gl.year)           AS year,
       COALESCE(dw.month,          gl.month)          AS month,
       COALESCE(dw.dw_total,0) AS dw_total,
       COALESCE(gl.gl_total,0) AS gl_total,
       COALESCE(dw.dw_total,0) - COALESCE(gl.gl_total,0) AS difference
FROM dw
FULL OUTER JOIN gl
  ON dw.account_number = gl.account_number
 AND dw.year           = gl.year
 AND dw.month          = gl.month; 