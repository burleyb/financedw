-- models/silver/fact/fact_deal_netsuite.sql
-- Silver layer fact table for NetSuite related deal aggregates
-- Pulls directly from bronze.ns tables and matches gold_big_deal structure

CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_netsuite (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal (VIN-based matching)
  netsuite_posting_date_key INT, -- FK to dim_date (based on ns_date)
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
  repo BOOLEAN, -- Repo flag

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer fact table for NetSuite financial aggregates pulled directly from bronze.ns tables'
PARTITIONED BY (netsuite_posting_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze.ns tables
MERGE INTO silver.finance.fact_deal_netsuite AS target
USING (
  -- Complex NetSuite aggregation logic from bigdealnscreation.py
  WITH PerVinAccounts AS (
    -- VIN-level accounts aggregation
    SELECT
      CASE
        WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
        ELSE CONCAT(MONTH(t.trandate), '-', YEAR(t.trandate), '-Missing Vin')
      END AS vin_key,
      MAX(t.trandate) AS ns_date,
      
      -- Revenue accounts from salesinvoiced
      COALESCE(SUM(CASE WHEN so.account = 236 THEN so.amount ELSE 0 END), 0) AS rev_reserve_4105,
      COALESCE(SUM(CASE WHEN so.account = 237 THEN so.amount ELSE 0 END), 0) AS vsc_rev_4110,
      COALESCE(SUM(CASE WHEN so.account = 479 THEN so.amount ELSE 0 END), 0) AS vsc_volume_bonus_rev_4110b,
      COALESCE(SUM(CASE WHEN so.account = 238 THEN so.amount ELSE 0 END), 0) AS gap_rev_4120,
      COALESCE(SUM(CASE WHEN so.account = 480 THEN so.amount ELSE 0 END), 0) AS gap_volume_bonus_rev_4120b,
      COALESCE(SUM(CASE WHEN so.account = 239 THEN so.amount ELSE 0 END), 0) AS doc_fees_rev_4130,
      COALESCE(SUM(CASE WHEN so.account = 451 THEN so.amount ELSE 0 END), 0) AS titling_fees_rev_4141,
      
      -- Expense accounts from transactionline
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 532 THEN tl.foreignamount ELSE 0 END), 0) AS payoff_variance_5400,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 533 THEN tl.foreignamount ELSE 0 END), 0) AS sales_tax_variance_5401,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 534 THEN tl.foreignamount ELSE 0 END), 0) AS registration_variance_5402,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 538 THEN tl.foreignamount ELSE 0 END), 0) AS customer_experience_5403,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 539 THEN tl.foreignamount ELSE 0 END), 0) AS penalties_5404,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 452 THEN tl.foreignamount ELSE 0 END), 0) AS titling_fees_5141,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 447 AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH') AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL) THEN tl.foreignamount ELSE 0 END), 0) AS bank_buyout_fees_5520,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 267 AND t.abbrevtype != 'PURCHORD' AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL) THEN tl.foreignamount ELSE 0 END), 0) AS vsc_cor_5110,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 498 THEN tl.foreignamount ELSE 0 END), 0) AS vsc_advance_5110a,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 269 AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH') AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL) THEN tl.foreignamount ELSE 0 END), 0) AS gap_cor_5120,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 499 THEN tl.foreignamount ELSE 0 END), 0) AS gap_advance_5120a,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 551 THEN tl.foreignamount ELSE 0 END), 0) AS repo_amount
      
    FROM bronze.ns.transaction t
    LEFT JOIN bronze.ns.salesinvoiced so ON t.id = so.transaction
    LEFT JOIN bronze.ns.transactionline tl ON t.id = tl.transaction
    WHERE t.trandate IS NOT NULL
    GROUP BY 
      CASE
        WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
        ELSE CONCAT(MONTH(t.trandate), '-', YEAR(t.trandate), '-Missing Vin')
      END
  ),
  
  AverageAccounts AS (
    -- Monthly average accounts for missing VINs
    SELECT
      MONTH(t.trandate) AS month,
      YEAR(t.trandate) AS year,
      COUNT(DISTINCT CASE WHEN so.amount >= 699.00 AND so.account = 239 AND LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno END) AS driver_count,
      
      -- Average revenue accounts
      COALESCE(SUM(CASE WHEN so.account = 474 THEN so.amount ELSE 0 END), 0) AS reserve_bonus_rev_4106_total,
      COALESCE(SUM(CASE WHEN so.account = 524 THEN so.amount ELSE 0 END), 0) AS reserve_chargeback_rev_4107_total,
      COALESCE(SUM(CASE WHEN so.account = 517 THEN so.amount ELSE 0 END), 0) AS doc_fees_chargeback_rev_4130c_total,
      COALESCE(SUM(CASE WHEN so.account = 546 THEN so.amount ELSE 0 END), 0) AS vsc_advance_rev_4110a_total,
      COALESCE(SUM(CASE WHEN so.account = 545 THEN so.amount ELSE 0 END), 0) AS vsc_cost_rev_4110c_total,
      COALESCE(SUM(CASE WHEN so.account = 544 THEN so.amount ELSE 0 END), 0) AS vsc_chargeback_rev_4111_total,
      COALESCE(SUM(CASE WHEN so.account = 547 THEN so.amount ELSE 0 END), 0) AS gap_advance_rev_4120a_total,
      COALESCE(SUM(CASE WHEN so.account = 548 THEN so.amount ELSE 0 END), 0) AS gap_cost_rev_4120c_total,
      COALESCE(SUM(CASE WHEN so.account = 549 THEN so.amount ELSE 0 END), 0) AS gap_chargeback_rev_4121_total,
      
      -- Average expense accounts
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 528 THEN tl.foreignamount ELSE 0 END), 0) AS funding_clerks_5301_total,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 552 THEN tl.foreignamount ELSE 0 END), 0) AS commission_5302_total,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 553 THEN tl.foreignamount ELSE 0 END), 0) AS sales_guarantee_5303_total,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 556 THEN tl.foreignamount ELSE 0 END), 0) AS ic_payoff_team_5304_total,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 564 THEN tl.foreignamount ELSE 0 END), 0) AS outbound_commission_5305_total,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 529 THEN tl.foreignamount ELSE 0 END), 0) AS title_clerks_5320_total,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 530 THEN tl.foreignamount ELSE 0 END), 0) AS direct_emp_benefits_5330_total,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 531 THEN tl.foreignamount ELSE 0 END), 0) AS direct_payroll_tax_5340_total,
      COALESCE(SUM(CASE WHEN tl.expenseaccount = 541 THEN tl.foreignamount ELSE 0 END), 0) AS postage_5510_total
      
    FROM bronze.ns.transaction t
    LEFT JOIN bronze.ns.salesinvoiced so ON t.id = so.transaction
    LEFT JOIN bronze.ns.transactionline tl ON t.id = tl.transaction
    WHERE t.trandate IS NOT NULL
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
  )
  
  SELECT DISTINCT
    pva.vin_key AS deal_key,
    CAST(DATE_FORMAT(pva.ns_date, 'yyyyMMdd') AS INT) AS netsuite_posting_date_key,
    CAST(DATE_FORMAT(pva.ns_date, 'HHmmss') AS INT) AS netsuite_posting_time_key,

    -- Convert to cents (multiply by 100)
    CAST(pva.rev_reserve_4105 * 100 AS BIGINT) AS rev_reserve_4105,
    CAST(COALESCE(aa.reserve_bonus_rev_4106_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS reserve_bonus_rev_4106,
    CAST(COALESCE(aa.reserve_chargeback_rev_4107_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS reserve_chargeback_rev_4107,
    CAST(pva.vsc_rev_4110 * 100 AS BIGINT) AS vsc_rev_4110,
    CAST(COALESCE(aa.vsc_advance_rev_4110a_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS vsc_advance_rev_4110a,
    CAST(pva.vsc_volume_bonus_rev_4110b * 100 AS BIGINT) AS vsc_volume_bonus_rev_4110b,
    CAST(COALESCE(aa.vsc_cost_rev_4110c_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS vsc_cost_rev_4110c,
    CAST(COALESCE(aa.vsc_chargeback_rev_4111_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS vsc_chargeback_rev_4111,
    CAST(pva.gap_rev_4120 * 100 AS BIGINT) AS gap_rev_4120,
    CAST(COALESCE(aa.gap_advance_rev_4120a_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS gap_advance_rev_4120a,
    CAST(pva.gap_volume_bonus_rev_4120b * 100 AS BIGINT) AS gap_volume_bonus_rev_4120b,
    CAST(COALESCE(aa.gap_cost_rev_4120c_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS gap_cost_rev_4120c,
    CAST(COALESCE(aa.gap_chargeback_rev_4121_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS gap_chargeback_rev_4121,
    CAST(pva.doc_fees_rev_4130 * 100 AS BIGINT) AS doc_fees_rev_4130,
    CAST(COALESCE(aa.doc_fees_chargeback_rev_4130c_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS doc_fees_chargeback_rev_4130c,
    CAST(pva.titling_fees_rev_4141 * 100 AS BIGINT) AS titling_fees_rev_4141,
    CAST(COALESCE(aa.funding_clerks_5301_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS funding_clerks_5301,
    CAST(COALESCE(aa.commission_5302_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS commission_5302,
    CAST(COALESCE(aa.sales_guarantee_5303_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS sales_guarantee_5303,
    CAST(COALESCE(aa.ic_payoff_team_5304_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS ic_payoff_team_5304,
    CAST(COALESCE(aa.outbound_commission_5305_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS outbound_commission_5305,
    CAST(COALESCE(aa.title_clerks_5320_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS title_clerks_5320,
    CAST(COALESCE(aa.direct_emp_benefits_5330_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS direct_emp_benefits_5330,
    CAST(COALESCE(aa.direct_payroll_tax_5340_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS direct_payroll_tax_5340,
    CAST(pva.payoff_variance_5400 * 100 AS BIGINT) AS payoff_variance_5400,
    CAST(pva.sales_tax_variance_5401 * 100 AS BIGINT) AS sales_tax_variance_5401,
    CAST(pva.registration_variance_5402 * 100 AS BIGINT) AS registration_variance_5402,
    CAST(pva.customer_experience_5403 * 100 AS BIGINT) AS customer_experience_5403,
    CAST(pva.penalties_5404 * 100 AS BIGINT) AS penalties_5404,
    CAST(COALESCE(aa.postage_5510_total / NULLIF(aa.driver_count, 0), 0) * 100 AS BIGINT) AS postage_5510,
    CAST(pva.bank_buyout_fees_5520 * 100 AS BIGINT) AS bank_buyout_fees_5520,
    CAST(pva.vsc_cor_5110 * 100 AS BIGINT) AS vsc_cor_5110,
    CAST(pva.vsc_advance_5110a * 100 AS BIGINT) AS vsc_advance_5110a,
    CAST(pva.gap_cor_5120 * 100 AS BIGINT) AS gap_cor_5120,
    CAST(pva.gap_advance_5120a * 100 AS BIGINT) AS gap_advance_5120a,
    
    -- Calculate gross profit and margin
    CAST((
      pva.rev_reserve_4105 + pva.vsc_rev_4110 + pva.vsc_volume_bonus_rev_4110b + 
      pva.gap_rev_4120 + pva.gap_volume_bonus_rev_4120b + pva.doc_fees_rev_4130 + 
      pva.titling_fees_rev_4141 - pva.vsc_cor_5110 - pva.vsc_advance_5110a - 
      pva.gap_cor_5120 - pva.gap_advance_5120a
    ) * 100 AS BIGINT) AS gross_profit,
    
    CAST(CASE 
      WHEN (pva.rev_reserve_4105 + pva.vsc_rev_4110 + pva.vsc_volume_bonus_rev_4110b + 
            pva.gap_rev_4120 + pva.gap_volume_bonus_rev_4120b + pva.doc_fees_rev_4130 + 
            pva.titling_fees_rev_4141) > 0 THEN
        ((pva.rev_reserve_4105 + pva.vsc_rev_4110 + pva.vsc_volume_bonus_rev_4110b + 
          pva.gap_rev_4120 + pva.gap_volume_bonus_rev_4120b + pva.doc_fees_rev_4130 + 
          pva.titling_fees_rev_4141 - pva.vsc_cor_5110 - pva.vsc_advance_5110a - 
          pva.gap_cor_5120 - pva.gap_advance_5120a) / 
         (pva.rev_reserve_4105 + pva.vsc_rev_4110 + pva.vsc_volume_bonus_rev_4110b + 
          pva.gap_rev_4120 + pva.gap_volume_bonus_rev_4120b + pva.doc_fees_rev_4130 + 
          pva.titling_fees_rev_4141)) * 10000
      ELSE 0
    END AS BIGINT) AS gross_margin,
    
    CASE WHEN pva.repo_amount > 0 THEN TRUE ELSE FALSE END AS repo,

    'bronze.ns.*' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
    
  FROM PerVinAccounts pva
  LEFT JOIN AverageAccounts aa ON MONTH(pva.ns_date) = aa.month AND YEAR(pva.ns_date) = aa.year
  WHERE pva.ns_date IS NOT NULL
  
  -- Filter for incremental updates
  AND pva.ns_date > COALESCE((SELECT MAX(DATE(ns_date)) FROM (
    SELECT DATE_FORMAT(TO_DATE(CAST(netsuite_posting_date_key AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd') AS ns_date 
    FROM silver.finance.fact_deal_netsuite
  )), '1900-01-01')

) AS source
ON target.deal_key = source.deal_key
   AND target.netsuite_posting_date_key = source.netsuite_posting_date_key

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