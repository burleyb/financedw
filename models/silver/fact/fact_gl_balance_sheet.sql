-- models/silver/fact/fact_gl_balance_sheet.sql
-- Balance-sheet fact layer: captures all Asset / Liability / Equity postings from NetSuite GL
-- Grain: one row per transaction-accounting-line (unique by transaction + line number)
-- Sign-convention: NetSuite native (debits positive, credits negative)

DROP TABLE IF EXISTS silver.finance.fact_gl_balance_sheet;

CREATE TABLE IF NOT EXISTS silver.finance.fact_gl_balance_sheet (
  gl_entry_key        STRING NOT NULL, -- transaction_id + line_id composite
  transaction_id      BIGINT,          -- NetSuite transaction internal ID
  transaction_line_id BIGINT,          -- NetSuite transactionaccountingline.line
  account_key         STRING NOT NULL, -- FK to dim_account
  posting_date_key    BIGINT,          -- yyyymmdd (based on trandate UTC)
  posting_time_key    BIGINT,          -- HHmmss  (trandate UTC)
  year                INT,
  month               INT,
  amount_cents        BIGINT,          -- debit = +, credit = –
  amount_dollars      DECIMAL(15,2),
  debit_credit        STRING,          -- 'DEBIT' / 'CREDIT'
  _source_table       STRING,
  _load_timestamp     TIMESTAMP
)
USING DELTA
PARTITIONED BY (year, month)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- Incremental merge (idempotent)
MERGE INTO silver.finance.fact_gl_balance_sheet AS tgt
USING (
  SELECT
    CONCAT_WS('_', tal.transaction, tal.transactionline)       AS gl_entry_key,
    tal.transaction                                            AS transaction_id,
    tal.transactionline                                        AS transaction_line_id,
    CAST(a.id AS STRING)                                       AS account_key,
    CAST(DATE_FORMAT(t.trandate,'yyyyMMdd') AS BIGINT)         AS posting_date_key,
    CAST(DATE_FORMAT(t.trandate,'HHmmss')   AS BIGINT)         AS posting_time_key,
    YEAR(t.trandate)                                           AS year,
    MONTH(t.trandate)                                          AS month,
    CAST(tal.amount * 100 AS BIGINT)                           AS amount_cents,
    CAST(tal.amount AS DECIMAL(15,2))                          AS amount_dollars,
    CASE WHEN tal.amount >= 0 THEN 'DEBIT' ELSE 'CREDIT' END   AS debit_credit,
    'bronze.ns.transactionaccountingline'                      AS _source_table,
    CURRENT_TIMESTAMP()                                        AS _load_timestamp
  FROM bronze.ns.transactionaccountingline tal
  JOIN bronze.ns.transaction t ON tal.transaction = t.id
  JOIN bronze.ns.account      a ON tal.account     = a.id
  WHERE tal.posting = 'T'
    AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
    AND (t._fivetran_deleted   = FALSE OR t._fivetran_deleted   IS NULL)
    -- Keep only balance-sheet accounts (already typed in dim_account mapping)
    AND UPPER(a.accttype) IN (
          'BANK','ACCREC','INVENTORY','OTHCURRASSET','FIXEDASSET','ACCUMDEPRECIATION','OTHERASSET',
          'ACCTSPAY','CREDITCARD','OTHCURRLIAB','LONGTERMLIAB',
          'EQUITY','RETEARNINGS')
) src
ON tgt.gl_entry_key = src.gl_entry_key
WHEN MATCHED THEN UPDATE SET
  tgt.account_key      = src.account_key,
  tgt.posting_date_key = src.posting_date_key,
  tgt.posting_time_key = src.posting_time_key,
  tgt.year             = src.year,
  tgt.month            = src.month,
  tgt.amount_cents     = src.amount_cents,
  tgt.amount_dollars   = src.amount_dollars,
  tgt.debit_credit     = src.debit_credit,
  tgt._load_timestamp  = src._load_timestamp
WHEN NOT MATCHED THEN INSERT *; 