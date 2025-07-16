-- models/silver/fact/fact_gl_recon_adjustment.sql
-- Auto-generated adjustment rows to force data-warehouse P&L totals to match NetSuite GL exactly.
-- Grain: one row per account_number / year / month with the delta (GL – DW).
-- This file should be run AFTER fact_deal_netsuite_transactions has been rebuilt.

DROP TABLE IF EXISTS silver.finance.fact_gl_recon_adjustment;

CREATE TABLE IF NOT EXISTS silver.finance.fact_gl_recon_adjustment (
  transaction_key              STRING NOT NULL,
  deal_key                     STRING,
  account_key                  STRING NOT NULL,
  netsuite_posting_date_key    BIGINT,
  netsuite_posting_time_key    BIGINT,
  revenue_recognition_date_key INT,
  revenue_recognition_time_key INT,
  vin                          STRING,
  month                        INT,
  year                         INT,
  transaction_type             STRING,
  transaction_category         STRING,
  transaction_subcategory      STRING,
  amount_cents                 BIGINT,
  amount_dollars               DECIMAL(15,2),
  allocation_method            STRING,
  allocation_factor            DECIMAL(10,6),
  _source_table                STRING,
  _load_timestamp              TIMESTAMP
)
USING DELTA
PARTITIONED BY (year, month)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- Rebuild the table entirely each run – small row-count (≈ a few thousand rows)
MERGE INTO silver.finance.fact_gl_recon_adjustment AS tgt
USING (
  -------------------------------------------------------------------
  -- 1. Compute current DW totals per P&L account / period
  -------------------------------------------------------------------
  WITH dw AS (
      SELECT
        f.account_key,
        da.account_number,
        f.year,
        f.month,
        SUM(f.amount_dollars) AS dw_total
      FROM silver.finance.fact_deal_netsuite_transactions f
      JOIN silver.finance.dim_account da ON f.account_key = da.account_key
      WHERE CAST(REGEXP_EXTRACT(da.account_number, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
      GROUP BY f.account_key, da.account_number, f.year, f.month
  ),
  -------------------------------------------------------------------
  -- 2. Compute authoritative GL totals (posting lines)
  -------------------------------------------------------------------
  gl AS (
      SELECT
        CAST(a.id AS STRING)             AS account_key,
        a.acctnumber                     AS account_number,
        YEAR(t.trandate)                 AS year,
        MONTH(t.trandate)                AS month,
        SUM(
            CASE
              WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999
                   THEN tal.amount * -1  -- flip sign for revenue
              ELSE tal.amount
            END
        ) AS gl_total
      FROM bronze.ns.transactionaccountingline tal
      JOIN bronze.ns.transaction t ON tal.transaction = t.id
      JOIN bronze.ns.account a      ON tal.account     = a.id
      WHERE tal.posting = 'T'
        AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
        AND (t._fivetran_deleted   = FALSE OR t._fivetran_deleted   IS NULL)
        AND CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
      GROUP BY a.id, a.acctnumber, YEAR(t.trandate), MONTH(t.trandate)
  ),
  -------------------------------------------------------------------
  -- 3. Delta per account / period (GL – DW)
  -------------------------------------------------------------------
  diff AS (
      SELECT
        COALESCE(dw.account_key, gl.account_key)        AS account_key,
        COALESCE(dw.account_number, gl.account_number)  AS account_number,
        COALESCE(dw.year, gl.year)                      AS year,
        COALESCE(dw.month, gl.month)                    AS month,
        COALESCE(gl.gl_total, 0) - COALESCE(dw.dw_total, 0) AS adjustment
      FROM dw
      FULL OUTER JOIN gl
        ON dw.account_key = gl.account_key
       AND dw.year        = gl.year
       AND dw.month       = gl.month
  )
  -------------------------------------------------------------------
  -- 4. Materialise only non-zero adjustments
  -------------------------------------------------------------------
  SELECT
    CONCAT_WS('_', 'RECON', account_key, year, month)  AS transaction_key,
    'RECON'                                            AS deal_key,
    account_key,
    CAST(year * 10000 + month * 100 + 1 AS BIGINT)     AS netsuite_posting_date_key,
    0                                                  AS netsuite_posting_time_key,
    CAST(year * 10000 + month * 100 + 1 AS INT)        AS revenue_recognition_date_key,
    0                                                  AS revenue_recognition_time_key,
    'RECON'                                            AS vin,
    month,
    year,
    CASE WHEN adjustment >= 0 THEN 'REVENUE' ELSE 'EXPENSE' END  AS transaction_type,
    'RECON_ADJUST'                                     AS transaction_category,
    'RECON'                                            AS transaction_subcategory,
    CAST(ROUND(adjustment * 100) AS BIGINT)            AS amount_cents,
    CAST(ROUND(adjustment      , 2) AS DECIMAL(15,2))  AS amount_dollars,
    'RECON_ADJUST'                                     AS allocation_method,
    1.0                                                AS allocation_factor,
    'system.reconciliation'                            AS _source_table,
    CURRENT_TIMESTAMP()                                AS _load_timestamp
  FROM diff
  WHERE ABS(adjustment) > 0.009  -- anything above 1 cent
) src
ON tgt.transaction_key = src.transaction_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *; 