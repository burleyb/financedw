-- models/gold/views/vw_gl_vs_dw_simple.sql
-- Simple reconciliation: Silver fact table vs NetSuite posted GL
-- This catches ALL processing issues including DIRECT method problems

CREATE OR REPLACE VIEW gold.finance.vw_gl_vs_dw_simple AS
WITH dw_totals AS (
    SELECT
        da.account_number,
        da.account_name,
        f.year,
        f.month,
        SUM(f.amount_dollars) AS dw_total,
        COUNT(*) as dw_transaction_count
    FROM silver.finance.fact_deal_netsuite_transactions f
    JOIN silver.finance.dim_account da ON f.account_key = da.account_key
    WHERE CAST(REGEXP_EXTRACT(da.account_number, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
    GROUP BY da.account_number, da.account_name, f.year, f.month
),
ns_posted AS (
    SELECT
        a.acctnumber AS account_number,
        a.fullname AS account_name,
        YEAR(t.trandate) AS year,
        MONTH(t.trandate) AS month,
        SUM(
            CASE
                WHEN a.acctnumber IN ('4107', '4111', '4121')  -- Chargeback accounts: flip to negative to match DW
                     THEN tal.amount * -1
                WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999
                     THEN tal.amount * -1  -- Flip other revenue accounts to positive to match fact table
                ELSE tal.amount            -- Keep expense accounts as-is (positive)
            END
        ) AS ns_posted_total,
        COUNT(*) as ns_transaction_count
    FROM bronze.ns.transactionaccountingline tal
    JOIN bronze.ns.transaction t ON tal.transaction = t.id
    JOIN bronze.ns.account a ON tal.account = a.id
    WHERE tal.posting = 'T'
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
    GROUP BY a.acctnumber, a.fullname, YEAR(t.trandate), MONTH(t.trandate)
)
SELECT
    COALESCE(dw.account_number, ns.account_number) AS account_number,
    COALESCE(dw.account_name, ns.account_name) AS account_name,
    COALESCE(dw.year, ns.year) AS year,
    COALESCE(dw.month, ns.month) AS month,
    
    COALESCE(dw.dw_total, 0) AS dw_total,
    COALESCE(ns.ns_posted_total, 0) AS ns_posted_total,
    COALESCE(dw.dw_total, 0) - COALESCE(ns.ns_posted_total, 0) AS difference,
    
    COALESCE(dw.dw_transaction_count, 0) AS dw_transaction_count,
    COALESCE(ns.ns_transaction_count, 0) AS ns_transaction_count,
    
    CASE 
        WHEN ABS(COALESCE(dw.dw_total, 0) - COALESCE(ns.ns_posted_total, 0)) < 0.01 THEN 'MATCH'
        WHEN dw.dw_total IS NULL THEN 'MISSING_IN_DW'
        WHEN ns.ns_posted_total IS NULL THEN 'MISSING_IN_NS'
        ELSE 'MISMATCH'
    END AS status

FROM dw_totals dw
FULL OUTER JOIN ns_posted ns
  ON dw.account_number = ns.account_number
 AND dw.year = ns.year
 AND dw.month = ns.month
ORDER BY ABS(COALESCE(dw.dw_total, 0) - COALESCE(ns.ns_posted_total, 0)) DESC; 