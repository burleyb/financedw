-- models/fact/fact_deal_netsuite.sql
-- Fact table for NetSuite related deal aggregates

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS finance_gold.finance.fact_deal_netsuite (
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

-- 2. Merge incremental changes
MERGE INTO finance_gold.finance.fact_deal_netsuite AS target
USING (
  SELECT
    d.id AS deal_key,
    CAST(DATE_FORMAT(d.ns_date, 'yyyyMMdd') AS INT) AS netsuite_posting_date_key,
    CAST(DATE_FORMAT(d.ns_date, 'HHmmss') AS INT) AS netsuite_posting_time_key,

    -- Measures (multiply currency by 100, percentages by 10000)
    CAST(d.`4105_rev_reserve` * 100 AS BIGINT) as rev_reserve_4105,
    CAST(d.reserve_bonus_rev_4106 * 100 AS BIGINT) as reserve_bonus_rev_4106,
    CAST(d.reserve_chargeback_rev_4107 * 100 AS BIGINT) as reserve_chargeback_rev_4107,
    CAST(d.vsc_rev_4110 * 100 AS BIGINT) as vsc_rev_4110,
    CAST(d.vsc_advance_rev_4110a * 100 AS BIGINT) as vsc_advance_rev_4110a,
    CAST(d.vsc_volume_bonus_rev_4110b * 100 AS BIGINT) as vsc_volume_bonus_rev_4110b,
    CAST(d.vsc_cost_rev_4110c * 100 AS BIGINT) as vsc_cost_rev_4110c,
    CAST(d.vsc_chargeback_rev_4111 * 100 AS BIGINT) as vsc_chargeback_rev_4111,
    CAST(d.gap_rev_4120 * 100 AS BIGINT) as gap_rev_4120,
    CAST(d.gap_advance_rev_4120a * 100 AS BIGINT) as gap_advance_rev_4120a,
    CAST(d.gap_volume_bonus_rev_4120b * 100 AS BIGINT) as gap_volume_bonus_rev_4120b,
    CAST(d.gap_cost_rev_4120c * 100 AS BIGINT) as gap_cost_rev_4120c,
    CAST(d.gap_chargeback_rev_4121 * 100 AS BIGINT) as gap_chargeback_rev_4121,
    CAST(d.doc_fees_rev_4130 * 100 AS BIGINT) as doc_fees_rev_4130,
    CAST(d.doc_fees_chargeback_rev_4130c * 100 AS BIGINT) as doc_fees_chargeback_rev_4130c,
    CAST(d.titling_fees_rev_4141 * 100 AS BIGINT) as titling_fees_rev_4141,
    CAST(d.funding_clerks_5301 * 100 AS BIGINT) as funding_clerks_5301,
    CAST(d.commission_5302 * 100 AS BIGINT) as commission_5302,
    CAST(d.sales_guarantee_5303 * 100 AS BIGINT) as sales_guarantee_5303,
    CAST(d.ic_payoff_team_5304 * 100 AS BIGINT) as ic_payoff_team_5304,
    CAST(d.outbound_commission_5305 * 100 AS BIGINT) as outbound_commission_5305,
    CAST(d.title_clerks_5320 * 100 AS BIGINT) as title_clerks_5320,
    CAST(d.direct_emp_benefits_5330 * 100 AS BIGINT) as direct_emp_benefits_5330,
    CAST(d.direct_payroll_tax_5340 * 100 AS BIGINT) as direct_payroll_tax_5340,
    CAST(d.payoff_variance_5400 * 100 AS BIGINT) as payoff_variance_5400,
    CAST(d.sales_tax_variance_5401 * 100 AS BIGINT) as sales_tax_variance_5401,
    CAST(d.registration_variance_5402 * 100 AS BIGINT) as registration_variance_5402,
    CAST(d.customer_experience_5403 * 100 AS BIGINT) as customer_experience_5403,
    CAST(d.penalties_5404 * 100 AS BIGINT) as penalties_5404,
    CAST(d.postage_5510 * 100 AS BIGINT) as postage_5510,
    CAST(d.bank_buyout_fees_5520 * 100 AS BIGINT) as bank_buyout_fees_5520,
    CAST(d.vsc_cor_5110 * 100 AS BIGINT) as vsc_cor_5110,
    CAST(d.vsc_advance_5110a * 100 AS BIGINT) as vsc_advance_5110a,
    CAST(d.gap_cor_5120 * 100 AS BIGINT) as gap_cor_5120,
    CAST(d.gap_advance_5120a * 100 AS BIGINT) as gap_advance_5120a,
    CAST(d.gross_profit * 100 AS BIGINT) as gross_profit,
    CAST(d.gross_margin * 10000 AS BIGINT) as gross_margin,
    CAST(d.repo AS BOOLEAN) as repo,

    'silver.deal.big_deal' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM silver.deal.big_deal d
  -- Filter for relevant deals (e.g., those with NetSuite data or recent updates)
  WHERE d.ns_date IS NOT NULL -- Filter based on the NetSuite date
  -- Optional: Add time-based filter for incremental loads
  -- AND d.ns_date > (SELECT MAX(_load_timestamp) FROM gold.finance.fact_deal_netsuite WHERE _load_timestamp IS NOT NULL)

  -- Ensure only the latest version of each deal based on ns_date is processed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY d.ns_date DESC) = 1

) AS source
ON target.deal_key = source.deal_key
   AND target.netsuite_posting_date_key = source.netsuite_posting_date_key

-- Update existing NetSuite records if amounts change
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

-- Insert new NetSuite records
WHEN NOT MATCHED THEN
  INSERT * 