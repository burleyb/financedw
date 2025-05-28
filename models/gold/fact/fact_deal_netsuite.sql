-- models/gold/fact/fact_deal_netsuite.sql
-- Gold layer deal NetSuite fact table with business enhancements
-- Now pulls from silver layer instead of big_deal

CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_netsuite (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal
  netsuite_posting_date_key INT, -- FK to dim_date (based on ns_date or similar)
  netsuite_posting_time_key INT, -- FK to dim_time

  -- Measures (Stored as BIGINT cents where applicable)
  rev_reserve_4105 BIGINT,
  reserve_bonus_rev_4106 BIGINT,
  reserve_chargeback_rev_4107 BIGINT,
  vsc_rev_4110 BIGINT,
  vsc_advance_rev_4110a BIGINT,
  vsc_volume_bonus_rev_4110b BIGINT,
  vsc_cost_rev_4110c BIGINT,
  vsc_chargeback_rev_4111 BIGINT,
  gap_rev_4120 BIGINT,
  gap_advance_rev_4120a BIGINT,
  gap_volume_bonus_rev_4120b BIGINT,
  gap_cost_rev_4120c BIGINT,
  gap_chargeback_rev_4121 BIGINT,
  doc_fees_rev_4130 BIGINT,
  doc_fees_chargeback_rev_4130c BIGINT,
  titling_fees_rev_4141 BIGINT,
  funding_clerks_5301 BIGINT,
  commission_5302 BIGINT,
  sales_guarantee_5303 BIGINT,
  ic_payoff_team_5304 BIGINT,
  outbound_commission_5305 BIGINT,
  title_clerks_5320 BIGINT,
  direct_emp_benefits_5330 BIGINT,
  direct_payroll_tax_5340 BIGINT,
  payoff_variance_5400 BIGINT,
  sales_tax_variance_5401 BIGINT,
  registration_variance_5402 BIGINT,
  customer_experience_5403 BIGINT,
  penalties_5404 BIGINT,
  postage_5510 BIGINT,
  bank_buyout_fees_5520 BIGINT,
  vsc_cor_5110 BIGINT,
  vsc_advance_5110a BIGINT,
  gap_cor_5120 BIGINT,
  gap_advance_5120a BIGINT,
  gross_profit BIGINT,
  gross_margin BIGINT, -- Percentage * 10000 (e.g., 12.34% stored as 1234)
  repo BOOLEAN, -- Assuming this is a flag

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Fact table storing NetSuite related financial aggregates for deals.'
PARTITIONED BY (netsuite_posting_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.fact_deal_netsuite AS target
USING (
  SELECT
    sfn.deal_key,
    sfn.netsuite_posting_date_key,
    sfn.netsuite_posting_time_key,
    
    -- Financial measures (already in cents from silver)
    sfn.rev_reserve_4105,
    sfn.reserve_bonus_rev_4106,
    sfn.reserve_chargeback_rev_4107,
    sfn.vsc_rev_4110,
    sfn.vsc_advance_rev_4110a,
    sfn.vsc_volume_bonus_rev_4110b,
    sfn.vsc_cost_rev_4110c,
    sfn.vsc_chargeback_rev_4111,
    sfn.gap_rev_4120,
    sfn.gap_advance_rev_4120a,
    sfn.gap_volume_bonus_rev_4120b,
    sfn.gap_cost_rev_4120c,
    sfn.gap_chargeback_rev_4121,
    sfn.doc_fees_rev_4130,
    sfn.doc_fees_chargeback_rev_4130c,
    sfn.titling_fees_rev_4141,
    sfn.funding_clerks_5301,
    sfn.commission_5302,
    sfn.sales_guarantee_5303,
    sfn.ic_payoff_team_5304,
    sfn.outbound_commission_5305,
    sfn.title_clerks_5320,
    sfn.direct_emp_benefits_5330,
    sfn.direct_payroll_tax_5340,
    sfn.payoff_variance_5400,
    sfn.sales_tax_variance_5401,
    sfn.registration_variance_5402,
    sfn.customer_experience_5403,
    sfn.penalties_5404,
    sfn.postage_5510,
    sfn.bank_buyout_fees_5520,
    sfn.vsc_cor_5110,
    sfn.vsc_advance_5110a,
    sfn.gap_cor_5120,
    sfn.gap_advance_5120a,
    sfn.gross_profit,
    sfn.gross_margin,
    sfn.repo,
    
    sfn._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  
  FROM silver.finance.fact_deal_netsuite sfn
  WHERE sfn._load_timestamp > COALESCE((SELECT MAX(_load_timestamp) FROM gold.finance.fact_deal_netsuite), '1900-01-01')

) AS source
ON target.deal_key = source.deal_key AND target.netsuite_posting_date_key = source.netsuite_posting_date_key

WHEN MATCHED THEN
  UPDATE SET
    target.netsuite_posting_time_key = source.netsuite_posting_time_key,
    target.rev_reserve_4105 = source.rev_reserve_4105,
    target.reserve_bonus_rev_4106 = source.reserve_bonus_rev_4106,
    target.reserve_chargeback_rev_4107 = source.reserve_chargeback_rev_4107,
    target.vsc_rev_4110 = source.vsc_rev_4110,
    target.vsc_advance_rev_4110a = source.vsc_advance_rev_4110a,
    target.vsc_volume_bonus_rev_4110b = source.vsc_volume_bonus_rev_4110b,
    target.vsc_cost_rev_4110c = source.vsc_cost_rev_4110c,
    target.vsc_chargeback_rev_4111 = source.vsc_chargeback_rev_4111,
    target.gap_rev_4120 = source.gap_rev_4120,
    target.gap_advance_rev_4120a = source.gap_advance_rev_4120a,
    target.gap_volume_bonus_rev_4120b = source.gap_volume_bonus_rev_4120b,
    target.gap_cost_rev_4120c = source.gap_cost_rev_4120c,
    target.gap_chargeback_rev_4121 = source.gap_chargeback_rev_4121,
    target.doc_fees_rev_4130 = source.doc_fees_rev_4130,
    target.doc_fees_chargeback_rev_4130c = source.doc_fees_chargeback_rev_4130c,
    target.titling_fees_rev_4141 = source.titling_fees_rev_4141,
    target.funding_clerks_5301 = source.funding_clerks_5301,
    target.commission_5302 = source.commission_5302,
    target.sales_guarantee_5303 = source.sales_guarantee_5303,
    target.ic_payoff_team_5304 = source.ic_payoff_team_5304,
    target.outbound_commission_5305 = source.outbound_commission_5305,
    target.title_clerks_5320 = source.title_clerks_5320,
    target.direct_emp_benefits_5330 = source.direct_emp_benefits_5330,
    target.direct_payroll_tax_5340 = source.direct_payroll_tax_5340,
    target.payoff_variance_5400 = source.payoff_variance_5400,
    target.sales_tax_variance_5401 = source.sales_tax_variance_5401,
    target.registration_variance_5402 = source.registration_variance_5402,
    target.customer_experience_5403 = source.customer_experience_5403,
    target.penalties_5404 = source.penalties_5404,
    target.postage_5510 = source.postage_5510,
    target.bank_buyout_fees_5520 = source.bank_buyout_fees_5520,
    target.vsc_cor_5110 = source.vsc_cor_5110,
    target.vsc_advance_5110a = source.vsc_advance_5110a,
    target.gap_cor_5120 = source.gap_cor_5120,
    target.gap_advance_5120a = source.gap_advance_5120a,
    target.gross_profit = source.gross_profit,
    target.gross_margin = source.gross_margin,
    target.repo = source.repo,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT *; 