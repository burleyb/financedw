-- models/gold/fact/fact_deal_commissions.sql
-- Gold layer fact table for deal commissions - exact copy of silver structure

DROP TABLE IF EXISTS gold.finance.fact_deal_commissions;

CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_commissions (
  deal_key STRING NOT NULL, -- FK to dim_deal
  employee_setter_key STRING, -- FK to dim_employee (if setter is tracked)
  employee_closer_key STRING, -- FK to dim_employee
  employee_closer2_key STRING, -- FK to dim_employee
  commission_date_key INT, -- FK to dim_date (likely completion date)
  commission_time_key INT, -- FK to dim_time (likely completion time)

  has_credit_memo BOOLEAN,
  credit_memo_date_key INT, -- FK to dim_date
  credit_memo_time_key INT, -- FK to dim_time

  setter_commission_amount BIGINT,
  closer_commission_amount BIGINT,
  closer2_commission_amount BIGINT,
  commissionable_gross_revenue_amount BIGINT,

  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer fact table storing commission amounts related to deals'
PARTITIONED BY (commission_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

MERGE INTO gold.finance.fact_deal_commissions AS target
USING (
  SELECT
    sfc.deal_key,
    sfc.employee_setter_key,
    sfc.employee_closer_key,
    sfc.employee_closer2_key,
    sfc.commission_date_key,
    sfc.commission_time_key,
    sfc.has_credit_memo,
    sfc.credit_memo_date_key,
    sfc.credit_memo_time_key,
    sfc.setter_commission_amount,
    sfc.closer_commission_amount,
    sfc.closer2_commission_amount,
    sfc.commissionable_gross_revenue_amount,
    sfc._source_table,
    sfc._load_timestamp
  FROM silver.finance.fact_deal_commissions sfc
) AS source
ON target.deal_key = source.deal_key
   AND target.commission_date_key = source.commission_date_key

WHEN MATCHED THEN
  UPDATE SET
    target.employee_setter_key = source.employee_setter_key,
    target.employee_closer_key = source.employee_closer_key,
    target.employee_closer2_key = source.employee_closer2_key,
    target.commission_time_key = source.commission_time_key,
    target.has_credit_memo = source.has_credit_memo,
    target.credit_memo_date_key = source.credit_memo_date_key,
    target.credit_memo_time_key = source.credit_memo_time_key,
    target.setter_commission_amount = source.setter_commission_amount,
    target.closer_commission_amount = source.closer_commission_amount,
    target.closer2_commission_amount = source.closer2_commission_amount,
    target.commissionable_gross_revenue_amount = source.commissionable_gross_revenue_amount,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    deal_key,
    employee_setter_key,
    employee_closer_key,
    employee_closer2_key,
    commission_date_key,
    commission_time_key,
    has_credit_memo,
    credit_memo_date_key,
    credit_memo_time_key,
    setter_commission_amount,
    closer_commission_amount,
    closer2_commission_amount,
    commissionable_gross_revenue_amount,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_key,
    source.employee_setter_key,
    source.employee_closer_key,
    source.employee_closer2_key,
    source.commission_date_key,
    source.commission_time_key,
    source.has_credit_memo,
    source.credit_memo_date_key,
    source.credit_memo_time_key,
    source.setter_commission_amount,
    source.closer_commission_amount,
    source.closer2_commission_amount,
    source.commissionable_gross_revenue_amount,
    source._source_table,
    source._load_timestamp
  ); 