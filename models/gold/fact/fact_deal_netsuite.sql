-- models/gold/fact/fact_deal_netsuite.sql
-- Gold layer deal NetSuite fact table with business enhancements
-- Now pulls from silver layer instead of big_deal

-- Drop and recreate table to ensure correct schema
DROP TABLE IF EXISTS gold.finance.fact_deal_netsuite;

CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_netsuite (
  -- Keys
  deal_key STRING NOT NULL,
  netsuite_posting_date_key BIGINT,
  netsuite_posting_time_key BIGINT,
  revenue_recognition_date_key INT,
  revenue_recognition_time_key INT,
  vin STRING,
  month INT,
  year INT,

  -- Revenue Measures (4000 accounts) - stored as BIGINT cents
  rev_reserve_4105 BIGINT,
  reserve_bonus_rev_4106 BIGINT,
  reserve_chargeback_rev_4107 BIGINT,
  reserve_total_rev BIGINT,
  
  vsc_rev_4110 BIGINT,
  vsc_advance_rev_4110a BIGINT,
  vsc_bonus_rev_4110b BIGINT,
  vsc_cost_rev_4110c BIGINT,
  vsc_chargeback_rev_4111 BIGINT,
  vsc_total_rev BIGINT,
  
  gap_rev_4120 BIGINT,
  gap_advance_rev_4120a BIGINT,
  gap_volume_bonus_rev_4120b BIGINT,
  gap_cost_rev_4120c BIGINT,
  gap_chargeback_rev_4121 BIGINT,
  gap_total_rev BIGINT,
  
  doc_fees_rev_4130 BIGINT,
  doc_fees_chargeback_rev_4130c BIGINT,
  titling_fees_rev_4141 BIGINT,
  doc_title_total_rev BIGINT,
  
  rebates_discounts_4190 BIGINT,
  total_revenue BIGINT,

  -- Cost Measures (5000 accounts) - stored as BIGINT cents
  funding_clerks_5301 BIGINT,
  commission_5302 BIGINT,
  sales_guarantee_5303 BIGINT,
  ic_payoff_team_5304 BIGINT,
  outbound_commission_5305 BIGINT,
  title_clerks_5320 BIGINT,
  direct_emp_benefits_5330 BIGINT,
  direct_payroll_tax_5340 BIGINT,
  direct_people_cost BIGINT,
  
  payoff_variance_5400 BIGINT,
  sales_tax_variance_5401 BIGINT,
  registration_variance_5402 BIGINT,
  customer_experience_5403 BIGINT,
  penalties_5404 BIGINT,
  payoff_variance_total BIGINT,
  
  postage_5510 BIGINT,
  bank_buyout_fees_5520 BIGINT,
  other_cor_total BIGINT,
  
  vsc_cor_5110 BIGINT,
  vsc_advance_5110a BIGINT,
  vsc_total_cost BIGINT,
  
  gap_cor_5120 BIGINT,
  gap_advance_5120a BIGINT,
  gap_total_cost BIGINT,
  
  titling_fees_5141 BIGINT,
  cor_total BIGINT,
  
  -- Calculated fields
  gross_profit BIGINT,
  gross_margin BIGINT, -- Percentage * 10000
  
  -- Repo amount (stored as BIGINT cents)
  repo BIGINT,
  
  -- Source information
  deal_source STRING,

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer fact table for NetSuite financial aggregates - mirrors bigdealnscreation.py logic'
PARTITIONED BY (revenue_recognition_date_key)
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
    sfn.revenue_recognition_date_key,
    sfn.revenue_recognition_time_key,
    sfn.vin,
    sfn.month,
    sfn.year,
    
    -- Revenue measures (already in cents from silver)
    sfn.rev_reserve_4105,
    sfn.reserve_bonus_rev_4106,
    sfn.reserve_chargeback_rev_4107,
    sfn.reserve_total_rev,
    
    sfn.vsc_rev_4110,
    sfn.vsc_advance_rev_4110a,
    sfn.vsc_bonus_rev_4110b,
    sfn.vsc_cost_rev_4110c,
    sfn.vsc_chargeback_rev_4111,
    sfn.vsc_total_rev,
    
    sfn.gap_rev_4120,
    sfn.gap_advance_rev_4120a,
    sfn.gap_volume_bonus_rev_4120b,
    sfn.gap_cost_rev_4120c,
    sfn.gap_chargeback_rev_4121,
    sfn.gap_total_rev,
    
    sfn.doc_fees_rev_4130,
    sfn.doc_fees_chargeback_rev_4130c,
    sfn.titling_fees_rev_4141,
    sfn.doc_title_total_rev,
    
    sfn.rebates_discounts_4190,
    sfn.total_revenue,
    
    -- Cost measures
    sfn.funding_clerks_5301,
    sfn.commission_5302,
    sfn.sales_guarantee_5303,
    sfn.ic_payoff_team_5304,
    sfn.outbound_commission_5305,
    sfn.title_clerks_5320,
    sfn.direct_emp_benefits_5330,
    sfn.direct_payroll_tax_5340,
    sfn.direct_people_cost,
    
    sfn.payoff_variance_5400,
    sfn.sales_tax_variance_5401,
    sfn.registration_variance_5402,
    sfn.customer_experience_5403,
    sfn.penalties_5404,
    sfn.payoff_variance_total,
    
    sfn.postage_5510,
    sfn.bank_buyout_fees_5520,
    sfn.other_cor_total,
    
    sfn.vsc_cor_5110,
    sfn.vsc_advance_5110a,
    sfn.vsc_total_cost,
    
    sfn.gap_cor_5120,
    sfn.gap_advance_5120a,
    sfn.gap_total_cost,
    
    sfn.titling_fees_5141,
    sfn.cor_total,
    
    -- Calculated fields
    sfn.gross_profit,
    sfn.gross_margin,
    
    -- Flags
    sfn.repo,
    
    -- Source information
    sfn.deal_source,
    
    -- Metadata
    sfn._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  
  FROM silver.finance.fact_deal_netsuite sfn
  WHERE sfn._load_timestamp > COALESCE((SELECT MAX(_load_timestamp) FROM gold.finance.fact_deal_netsuite), '1900-01-01')

) AS source
ON target.deal_key = source.deal_key

WHEN MATCHED AND (
    target.revenue_recognition_date_key != source.revenue_recognition_date_key OR
    target.netsuite_posting_date_key != source.netsuite_posting_date_key OR
    target.total_revenue != source.total_revenue OR
    target.gross_profit != source.gross_profit
) THEN
  UPDATE SET
    target.netsuite_posting_date_key = source.netsuite_posting_date_key,
    target.netsuite_posting_time_key = source.netsuite_posting_time_key,
    target.revenue_recognition_date_key = source.revenue_recognition_date_key,
    target.revenue_recognition_time_key = source.revenue_recognition_time_key,
    target.vin = source.vin,
    target.month = source.month,
    target.year = source.year,
    target.rev_reserve_4105 = source.rev_reserve_4105,
    target.reserve_bonus_rev_4106 = source.reserve_bonus_rev_4106,
    target.reserve_chargeback_rev_4107 = source.reserve_chargeback_rev_4107,
    target.reserve_total_rev = source.reserve_total_rev,
    target.vsc_rev_4110 = source.vsc_rev_4110,
    target.vsc_advance_rev_4110a = source.vsc_advance_rev_4110a,
    target.vsc_bonus_rev_4110b = source.vsc_bonus_rev_4110b,
    target.vsc_cost_rev_4110c = source.vsc_cost_rev_4110c,
    target.vsc_chargeback_rev_4111 = source.vsc_chargeback_rev_4111,
    target.vsc_total_rev = source.vsc_total_rev,
    target.gap_rev_4120 = source.gap_rev_4120,
    target.gap_advance_rev_4120a = source.gap_advance_rev_4120a,
    target.gap_volume_bonus_rev_4120b = source.gap_volume_bonus_rev_4120b,
    target.gap_cost_rev_4120c = source.gap_cost_rev_4120c,
    target.gap_chargeback_rev_4121 = source.gap_chargeback_rev_4121,
    target.gap_total_rev = source.gap_total_rev,
    target.doc_fees_rev_4130 = source.doc_fees_rev_4130,
    target.doc_fees_chargeback_rev_4130c = source.doc_fees_chargeback_rev_4130c,
    target.titling_fees_rev_4141 = source.titling_fees_rev_4141,
    target.doc_title_total_rev = source.doc_title_total_rev,
    target.rebates_discounts_4190 = source.rebates_discounts_4190,
    target.total_revenue = source.total_revenue,
    target.funding_clerks_5301 = source.funding_clerks_5301,
    target.commission_5302 = source.commission_5302,
    target.sales_guarantee_5303 = source.sales_guarantee_5303,
    target.ic_payoff_team_5304 = source.ic_payoff_team_5304,
    target.outbound_commission_5305 = source.outbound_commission_5305,
    target.title_clerks_5320 = source.title_clerks_5320,
    target.direct_emp_benefits_5330 = source.direct_emp_benefits_5330,
    target.direct_payroll_tax_5340 = source.direct_payroll_tax_5340,
    target.direct_people_cost = source.direct_people_cost,
    target.payoff_variance_5400 = source.payoff_variance_5400,
    target.sales_tax_variance_5401 = source.sales_tax_variance_5401,
    target.registration_variance_5402 = source.registration_variance_5402,
    target.customer_experience_5403 = source.customer_experience_5403,
    target.penalties_5404 = source.penalties_5404,
    target.payoff_variance_total = source.payoff_variance_total,
    target.postage_5510 = source.postage_5510,
    target.bank_buyout_fees_5520 = source.bank_buyout_fees_5520,
    target.other_cor_total = source.other_cor_total,
    target.vsc_cor_5110 = source.vsc_cor_5110,
    target.vsc_advance_5110a = source.vsc_advance_5110a,
    target.vsc_total_cost = source.vsc_total_cost,
    target.gap_cor_5120 = source.gap_cor_5120,
    target.gap_advance_5120a = source.gap_advance_5120a,
    target.gap_total_cost = source.gap_total_cost,
    target.titling_fees_5141 = source.titling_fees_5141,
    target.cor_total = source.cor_total,
    target.gross_profit = source.gross_profit,
    target.gross_margin = source.gross_margin,
    target.repo = source.repo,
    target.deal_source = source.deal_source,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    deal_key,
    netsuite_posting_date_key,
    netsuite_posting_time_key,
    revenue_recognition_date_key,
    revenue_recognition_time_key,
    vin,
    month,
    year,
    rev_reserve_4105,
    reserve_bonus_rev_4106,
    reserve_chargeback_rev_4107,
    reserve_total_rev,
    vsc_rev_4110,
    vsc_advance_rev_4110a,
    vsc_bonus_rev_4110b,
    vsc_cost_rev_4110c,
    vsc_chargeback_rev_4111,
    vsc_total_rev,
    gap_rev_4120,
    gap_advance_rev_4120a,
    gap_volume_bonus_rev_4120b,
    gap_cost_rev_4120c,
    gap_chargeback_rev_4121,
    gap_total_rev,
    doc_fees_rev_4130,
    doc_fees_chargeback_rev_4130c,
    titling_fees_rev_4141,
    doc_title_total_rev,
    rebates_discounts_4190,
    total_revenue,
    funding_clerks_5301,
    commission_5302,
    sales_guarantee_5303,
    ic_payoff_team_5304,
    outbound_commission_5305,
    title_clerks_5320,
    direct_emp_benefits_5330,
    direct_payroll_tax_5340,
    direct_people_cost,
    payoff_variance_5400,
    sales_tax_variance_5401,
    registration_variance_5402,
    customer_experience_5403,
    penalties_5404,
    payoff_variance_total,
    postage_5510,
    bank_buyout_fees_5520,
    other_cor_total,
    vsc_cor_5110,
    vsc_advance_5110a,
    vsc_total_cost,
    gap_cor_5120,
    gap_advance_5120a,
    gap_total_cost,
    titling_fees_5141,
    cor_total,
    gross_profit,
    gross_margin,
    repo,
    deal_source,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_key,
    source.netsuite_posting_date_key,
    source.netsuite_posting_time_key,
    source.revenue_recognition_date_key,
    source.revenue_recognition_time_key,
    source.vin,
    source.month,
    source.year,
    source.rev_reserve_4105,
    source.reserve_bonus_rev_4106,
    source.reserve_chargeback_rev_4107,
    source.reserve_total_rev,
    source.vsc_rev_4110,
    source.vsc_advance_rev_4110a,
    source.vsc_bonus_rev_4110b,
    source.vsc_cost_rev_4110c,
    source.vsc_chargeback_rev_4111,
    source.vsc_total_rev,
    source.gap_rev_4120,
    source.gap_advance_rev_4120a,
    source.gap_volume_bonus_rev_4120b,
    source.gap_cost_rev_4120c,
    source.gap_chargeback_rev_4121,
    source.gap_total_rev,
    source.doc_fees_rev_4130,
    source.doc_fees_chargeback_rev_4130c,
    source.titling_fees_rev_4141,
    source.doc_title_total_rev,
    source.rebates_discounts_4190,
    source.total_revenue,
    source.funding_clerks_5301,
    source.commission_5302,
    source.sales_guarantee_5303,
    source.ic_payoff_team_5304,
    source.outbound_commission_5305,
    source.title_clerks_5320,
    source.direct_emp_benefits_5330,
    source.direct_payroll_tax_5340,
    source.direct_people_cost,
    source.payoff_variance_5400,
    source.sales_tax_variance_5401,
    source.registration_variance_5402,
    source.customer_experience_5403,
    source.penalties_5404,
    source.payoff_variance_total,
    source.postage_5510,
    source.bank_buyout_fees_5520,
    source.other_cor_total,
    source.vsc_cor_5110,
    source.vsc_advance_5110a,
    source.vsc_total_cost,
    source.gap_cor_5120,
    source.gap_advance_5120a,
    source.gap_total_cost,
    source.titling_fees_5141,
    source.cor_total,
    source.gross_profit,
    source.gross_margin,
    source.repo,
    source.deal_source,
    source._source_table,
    source._load_timestamp
  ); 