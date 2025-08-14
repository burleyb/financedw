-- gold.finance.vw_gl_to_dw_line_gaps
-- Line-level diagnostic to find posted GL lines not present in DW posted facts (and duplicates)

CREATE OR REPLACE VIEW gold.finance.vw_gl_to_dw_line_gaps AS
WITH gl_posted AS (
  SELECT
    t.id AS transaction_id,
    tal.transactionline AS gl_line_id,
    a.acctnumber AS account_number,
    tal.account AS account_id,
    tal.amount AS gl_amount,
    DATE(t.trandate) AS gl_date,
    YEAR(t.trandate) AS year,
    MONTH(t.trandate) AS month
  FROM bronze.ns.transactionaccountingline tal
  JOIN bronze.ns.transaction t ON tal.transaction = t.id
  JOIN bronze.ns.account a ON tal.account = a.id
  WHERE tal.posting = 'T'
    AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
    AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
    AND CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
), dw_posted AS (
  SELECT
    -- We don't have the GL line id, so match by transaction+account+amount rounded
    f.account_key,
    CAST(f.account_key AS BIGINT) AS account_id,
    f.netsuite_posting_date_key,
    f.amount_dollars,
    CAST(ROUND(f.amount_dollars, 2) AS DECIMAL(15,2)) AS amount_rounded,
    f.transaction_key
  FROM gold.finance.fact_deal_netsuite_transactions f
)
SELECT
  gp.account_number,
  gp.year,
  gp.month,
  gp.transaction_id,
  gp.gl_line_id,
  gp.gl_amount,
  dwp.transaction_key AS matched_transaction_key
FROM gl_posted gp
LEFT JOIN dw_posted dwp
  ON gp.account_id = dwp.account_id
  AND CAST(REPLACE(CAST(gp.gl_date AS STRING), '-', '') AS BIGINT) = dwp.netsuite_posting_date_key
  AND CAST(ROUND(gp.gl_amount, 2) AS DECIMAL(15,2)) = dwp.amount_rounded
WHERE dwp.transaction_key IS NULL
ORDER BY ABS(gp.gl_amount) DESC;



