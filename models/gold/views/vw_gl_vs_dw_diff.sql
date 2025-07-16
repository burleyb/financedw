-- models/gold/views/vw_gl_vs_dw_diff.sql
-- Finance reconciliation view: compares Delta warehouse fact totals with raw NetSuite GL by account & month
-- Usage: SELECT * FROM gold.finance.vw_gl_vs_dw_diff WHERE year = 2025 AND month = 1 ORDER BY ABS(difference) DESC;



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
ns AS (
    SELECT
        a.acctnumber  AS account_number,
        a.fullname    AS account_name,
        YEAR(t.trandate)  AS year,
        MONTH(t.trandate) AS month,
        -- Sum GL amounts; credits are negative, debits positive.  We leave sign "as-is" so
        -- later comparison uses DW-sign convention (revenue +, expense –)
        SUM(
       CASE
         WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999
              THEN tal.amount * -1      -- flip sign for revenue accounts
         ELSE tal.amount
       END
   ) AS gl_total
    FROM bronze.ns.transactionaccountingline tal
    JOIN bronze.ns.transaction t ON tal.transaction = t.id
    JOIN bronze.ns.account a        ON tal.account    = a.id
    WHERE tal.posting = 'T'
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND (t._fivetran_deleted   = FALSE OR t._fivetran_deleted   IS NULL)
      AND CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
    GROUP BY a.acctnumber, a.fullname, YEAR(t.trandate), MONTH(t.trandate)
)
SELECT
    COALESCE(dw.account_number, ns.account_number) AS account_number,
    COALESCE(dw.account_name,   ns.account_name)   AS account_name,
    COALESCE(dw.year,           ns.year)           AS year,
    COALESCE(dw.month,          ns.month)          AS month,
    COALESCE(dw.dw_total, 0)  AS dw_total,
    COALESCE(ns.gl_total, 0)  AS ns_total,
    COALESCE(dw.dw_total, 0) - COALESCE(ns.gl_total, 0) AS difference
FROM (
    SELECT account_number, year, month, SUM(amount_dollars) AS dw_total
    FROM (
        SELECT da.account_number, dt.year, dt.month, f.amount_dollars
        FROM silver.finance.fact_deal_netsuite_transactions f
        JOIN silver.finance.dim_account da ON f.account_key = da.account_key
        JOIN silver.finance.dim_date dt ON f.revenue_recognition_date_key = dt.date_key
        WHERE CAST(REGEXP_EXTRACT(da.account_number, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999

        UNION ALL
        -- Reconciliation adjustments
        SELECT da.account_number, fa.year, fa.month, fa.amount_dollars
        FROM silver.finance.fact_gl_recon_adjustment fa
        JOIN silver.finance.dim_account da ON fa.account_key = da.account_key
        WHERE CAST(REGEXP_EXTRACT(da.account_number, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
    ) dw_sub
    GROUP BY account_number, year, month
) dw
FULL OUTER JOIN ns
  ON dw.account_number = ns.account_number
 AND dw.year           = ns.year
 AND dw.month          = ns.month; 