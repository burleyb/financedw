-- models/silver/fact/fact_deal_netsuite.sql
-- NetSuite fact table with period-based missing VIN allocation

-- Drop and recreate table to ensure correct schema
DROP TABLE IF EXISTS silver.finance.fact_deal_netsuite;

-- 1. Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_netsuite (
  deal_key STRING NOT NULL,
  netsuite_posting_date_key BIGINT,
  netsuite_posting_time_key BIGINT,
  revenue_recognition_date_key INT,
  revenue_recognition_time_key INT,
  vin STRING,
  month INT,
  year INT,
  
  -- Revenue fields (stored as BIGINT cents)
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
  
  -- Expense fields (stored as BIGINT cents)
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
  gross_profit BIGINT,
  gross_margin BIGINT,
  repo BIGINT,
  deal_source STRING,
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (year, month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO silver.finance.fact_deal_netsuite AS target
USING (
  WITH revenue_recognition_data AS (
    SELECT
        ds.deal_id,
        ds.updated_date_utc as revenue_recognition_date_utc,
        DATE_FORMAT(ds.updated_date_utc, 'yyyy-MM') as revenue_recognition_period,
        YEAR(ds.updated_date_utc) as revenue_recognition_year,
        MONTH(ds.updated_date_utc) as revenue_recognition_month,
        QUARTER(ds.updated_date_utc) as revenue_recognition_quarter,
        ROW_NUMBER() OVER (PARTITION BY ds.deal_id ORDER BY ds.updated_date_utc ASC) as rn
    FROM bronze.leaseend_db_public.deal_states ds
    WHERE ds.state = 'signed' 
        AND ds.deal_id IS NOT NULL 
        AND (ds._fivetran_deleted = FALSE OR ds._fivetran_deleted IS NULL)
  ),
  
  deal_vins AS (
    SELECT DISTINCT
        c.deal_id,
        UPPER(c.vin) as vin
    FROM bronze.leaseend_db_public.cars c
    WHERE c.vin IS NOT NULL 
        AND LENGTH(c.vin) = 17
        AND c.deal_id IS NOT NULL
        AND (c._fivetran_deleted = FALSE OR c._fivetran_deleted IS NULL)
  ),
  
  revenue_recognition_with_vins AS (
    SELECT 
        frr.deal_id,
        frr.revenue_recognition_date_utc,
        frr.revenue_recognition_period,
        frr.revenue_recognition_year,
        frr.revenue_recognition_month,
        frr.revenue_recognition_quarter,
        dv.vin
    FROM revenue_recognition_data frr
    INNER JOIN deal_vins dv ON frr.deal_id = dv.deal_id
    WHERE frr.rn = 1
  ),
  
  deals_per_period AS (
    SELECT 
        revenue_recognition_period,
        COUNT(DISTINCT deal_id) as deal_count
    FROM revenue_recognition_with_vins
    GROUP BY revenue_recognition_period
  ),
  
  -- VIN-matching revenue accounts (transactions with valid VINs)
  vin_matching_reserves AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 236
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_vsc_revenue AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 237
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_vsc_bonus AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 479
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_gap_revenue AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 238
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_gap_volume_bonus AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 480
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_doc_fees AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 239
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_titling_fees AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 451
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  -- VIN-matching expense accounts (transactions with valid VINs)
  vin_matching_payoff_variance AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 532
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_sales_tax_variance AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 533
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_registration_variance AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 534
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_customer_experience AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 538
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_penalties AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 539
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_titling_fees_expense AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 452
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_bank_buyout_fees AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 447
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.abbrevtype != 'PURCHORD'
        AND t.abbrevtype != 'SALESORD'
        AND t.abbrevtype != 'RTN AUTH'
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_vsc_cor AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 267
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.abbrevtype != 'PURCHORD'
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_vsc_advance_expense AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 498
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_gap_cor AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 269
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.abbrevtype != 'PURCHORD'
        AND t.abbrevtype != 'SALESORD'
        AND t.abbrevtype != 'RTN AUTH'
        AND t.abbrevtype != 'VENDAUTH'
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_gap_advance_expense AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 499
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  vin_matching_repo_amount AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 551
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  -- Missing VIN revenue accounts by period
  missing_vin_reserves AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 236
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_reserve_bonus AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 474
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_reserve_chargeback AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 524
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_vsc_advance AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 546
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_vsc_cost AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 545
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_vsc_chargeback AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 544
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_gap_advance AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 547
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_gap_cost AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 548
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_gap_chargeback AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 549
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_doc_chargeback AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 517
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_rebates AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 563
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_titling_fees AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 451
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  -- Missing VIN expense accounts by period
  missing_vin_funding_clerks AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 528
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  missing_vin_commission AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 552
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  -- Final allocation to deals
  allocated_amounts AS (
    SELECT
        rrwv.deal_id,
        rrwv.vin,
        rrwv.revenue_recognition_date_utc as ns_date,
        rrwv.revenue_recognition_month as month,
        rrwv.revenue_recognition_year as year,
        
        -- Allocate missing VIN amounts proportionally
        COALESCE(mvr.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_reserve_bonus,
        COALESCE(mvrc.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_reserve_chargeback,
        COALESCE(mvva.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_vsc_advance,
        COALESCE(mvvc.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_vsc_cost,
        COALESCE(mvvch.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_vsc_chargeback,
        COALESCE(mvga.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_gap_advance,
        COALESCE(mvgc.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_gap_cost,
        COALESCE(mvgch.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_gap_chargeback,
        COALESCE(mvdc.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_doc_chargeback,
        COALESCE(mvreb.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_rebates,
        COALESCE(mvtf.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_titling_fees,
        COALESCE(mvfc.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_funding_clerks,
        COALESCE(mvcom.total_amount / NULLIF(dpp.deal_count, 0), 0) as allocated_commission
        
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    LEFT JOIN missing_vin_reserve_bonus mvr ON rrwv.revenue_recognition_period = mvr.transaction_period
    LEFT JOIN missing_vin_reserve_chargeback mvrc ON rrwv.revenue_recognition_period = mvrc.transaction_period
    LEFT JOIN missing_vin_vsc_advance mvva ON rrwv.revenue_recognition_period = mvva.transaction_period
    LEFT JOIN missing_vin_vsc_cost mvvc ON rrwv.revenue_recognition_period = mvvc.transaction_period
    LEFT JOIN missing_vin_vsc_chargeback mvvch ON rrwv.revenue_recognition_period = mvvch.transaction_period
    LEFT JOIN missing_vin_gap_advance mvga ON rrwv.revenue_recognition_period = mvga.transaction_period
    LEFT JOIN missing_vin_gap_cost mvgc ON rrwv.revenue_recognition_period = mvgc.transaction_period
    LEFT JOIN missing_vin_gap_chargeback mvgch ON rrwv.revenue_recognition_period = mvgch.transaction_period
    LEFT JOIN missing_vin_doc_chargeback mvdc ON rrwv.revenue_recognition_period = mvdc.transaction_period
    LEFT JOIN missing_vin_rebates mvreb ON rrwv.revenue_recognition_period = mvreb.transaction_period
    LEFT JOIN missing_vin_titling_fees mvtf ON rrwv.revenue_recognition_period = mvtf.transaction_period
    LEFT JOIN missing_vin_funding_clerks mvfc ON rrwv.revenue_recognition_period = mvfc.transaction_period
    LEFT JOIN missing_vin_commission mvcom ON rrwv.revenue_recognition_period = mvcom.transaction_period
  )
  
  SELECT
    CAST(aa.deal_id AS STRING) AS deal_key,
    COALESCE(CAST(DATE_FORMAT(aa.ns_date, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
    COALESCE(CAST(DATE_FORMAT(aa.ns_date, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
    COALESCE(CAST(DATE_FORMAT(aa.ns_date, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
    COALESCE(CAST(DATE_FORMAT(aa.ns_date, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
    aa.vin,
    aa.month as month,
    aa.year as year,
    
    -- Revenue fields (convert to cents) - combine VIN-matching + allocated missing VIN amounts
    CAST(ROUND(COALESCE(vmr.total_amount, 0) * 100) AS BIGINT) AS rev_reserve_4105,
    CAST(ROUND(aa.allocated_reserve_bonus * 100) AS BIGINT) AS reserve_bonus_rev_4106,
    CAST(ROUND(aa.allocated_reserve_chargeback * 100) AS BIGINT) AS reserve_chargeback_rev_4107,
    CAST(ROUND((COALESCE(vmr.total_amount, 0) + aa.allocated_reserve_bonus + aa.allocated_reserve_chargeback) * 100) AS BIGINT) AS reserve_total_rev,
    CAST(ROUND(COALESCE(vmvr.total_amount, 0) * 100) AS BIGINT) AS vsc_rev_4110,
    CAST(ROUND(aa.allocated_vsc_advance * 100) AS BIGINT) AS vsc_advance_rev_4110a,
    CAST(ROUND(COALESCE(vmvb.total_amount, 0) * 100) AS BIGINT) AS vsc_bonus_rev_4110b,
    CAST(ROUND(aa.allocated_vsc_cost * 100) AS BIGINT) AS vsc_cost_rev_4110c,
    CAST(ROUND(aa.allocated_vsc_chargeback * 100) AS BIGINT) AS vsc_chargeback_rev_4111,
    CAST(ROUND((COALESCE(vmvr.total_amount, 0) + aa.allocated_vsc_advance + COALESCE(vmvb.total_amount, 0) + aa.allocated_vsc_cost + aa.allocated_vsc_chargeback) * 100) AS BIGINT) AS vsc_total_rev,
    CAST(ROUND(COALESCE(vmgr.total_amount, 0) * 100) AS BIGINT) AS gap_rev_4120,
    CAST(ROUND(aa.allocated_gap_advance * 100) AS BIGINT) AS gap_advance_rev_4120a,
    CAST(ROUND(COALESCE(vmgvb.total_amount, 0) * 100) AS BIGINT) AS gap_volume_bonus_rev_4120b,
    CAST(ROUND(aa.allocated_gap_cost * 100) AS BIGINT) AS gap_cost_rev_4120c,
    CAST(ROUND(aa.allocated_gap_chargeback * 100) AS BIGINT) AS gap_chargeback_rev_4121,
    CAST(ROUND((COALESCE(vmgr.total_amount, 0) + aa.allocated_gap_advance + COALESCE(vmgvb.total_amount, 0) + aa.allocated_gap_cost + aa.allocated_gap_chargeback) * 100) AS BIGINT) AS gap_total_rev,
    CAST(ROUND(COALESCE(vmdf.total_amount, 0) * 100) AS BIGINT) AS doc_fees_rev_4130,
    CAST(ROUND(aa.allocated_doc_chargeback * 100) AS BIGINT) AS doc_fees_chargeback_rev_4130c,
    CAST(ROUND((COALESCE(vmtf.total_amount, 0) + aa.allocated_titling_fees) * 100) AS BIGINT) AS titling_fees_rev_4141,
    CAST(ROUND((COALESCE(vmdf.total_amount, 0) + aa.allocated_doc_chargeback + COALESCE(vmtf.total_amount, 0) + aa.allocated_titling_fees) * 100) AS BIGINT) AS doc_title_total_rev,
    CAST(ROUND(aa.allocated_rebates * 100) AS BIGINT) AS rebates_discounts_4190,
    CAST(ROUND((
      COALESCE(vmr.total_amount, 0) + aa.allocated_reserve_bonus + aa.allocated_reserve_chargeback +
      COALESCE(vmvr.total_amount, 0) + aa.allocated_vsc_advance + COALESCE(vmvb.total_amount, 0) + aa.allocated_vsc_cost + aa.allocated_vsc_chargeback +
      COALESCE(vmgr.total_amount, 0) + aa.allocated_gap_advance + COALESCE(vmgvb.total_amount, 0) + aa.allocated_gap_cost + aa.allocated_gap_chargeback +
      COALESCE(vmdf.total_amount, 0) + aa.allocated_doc_chargeback + COALESCE(vmtf.total_amount, 0) + aa.allocated_titling_fees +
      aa.allocated_rebates
    ) * 100) AS BIGINT) AS total_revenue,
    
    -- Expense fields (convert to cents) - combine VIN-matching + allocated missing VIN amounts
    CAST(ROUND(aa.allocated_funding_clerks * 100) AS BIGINT) AS funding_clerks_5301,
    CAST(ROUND(aa.allocated_commission * 100) AS BIGINT) AS commission_5302,
    CAST(0 AS BIGINT) AS sales_guarantee_5303, -- No missing VIN query for this yet
    CAST(0 AS BIGINT) AS ic_payoff_team_5304, -- No missing VIN query for this yet
    CAST(0 AS BIGINT) AS outbound_commission_5305, -- No missing VIN query for this yet
    CAST(0 AS BIGINT) AS title_clerks_5320, -- No missing VIN query for this yet
    CAST(0 AS BIGINT) AS direct_emp_benefits_5330, -- No missing VIN query for this yet
    CAST(0 AS BIGINT) AS direct_payroll_tax_5340, -- No missing VIN query for this yet
    CAST(ROUND((aa.allocated_funding_clerks + aa.allocated_commission) * 100) AS BIGINT) AS direct_people_cost,
    CAST(ROUND(COALESCE(vmpv.total_amount, 0) * 100) AS BIGINT) AS payoff_variance_5400,
    CAST(ROUND(COALESCE(vmstv.total_amount, 0) * 100) AS BIGINT) AS sales_tax_variance_5401,
    CAST(ROUND(COALESCE(vmrv.total_amount, 0) * 100) AS BIGINT) AS registration_variance_5402,
    CAST(ROUND(COALESCE(vmce.total_amount, 0) * 100) AS BIGINT) AS customer_experience_5403,
    CAST(ROUND(COALESCE(vmp.total_amount, 0) * 100) AS BIGINT) AS penalties_5404,
    CAST(ROUND((COALESCE(vmpv.total_amount, 0) + COALESCE(vmstv.total_amount, 0) + COALESCE(vmrv.total_amount, 0) + COALESCE(vmce.total_amount, 0) + COALESCE(vmp.total_amount, 0)) * 100) AS BIGINT) AS payoff_variance_total,
    CAST(0 AS BIGINT) AS postage_5510, -- No missing VIN query for this yet
    CAST(ROUND(COALESCE(vmbbf.total_amount, 0) * 100) AS BIGINT) AS bank_buyout_fees_5520,
    CAST(ROUND(COALESCE(vmbbf.total_amount, 0) * 100) AS BIGINT) AS other_cor_total,
    CAST(ROUND(COALESCE(vmvc.total_amount, 0) * 100) AS BIGINT) AS vsc_cor_5110,
    CAST(ROUND(COALESCE(vmvae.total_amount, 0) * 100) AS BIGINT) AS vsc_advance_5110a,
    CAST(ROUND((COALESCE(vmvc.total_amount, 0) + COALESCE(vmvae.total_amount, 0)) * 100) AS BIGINT) AS vsc_total_cost,
    CAST(ROUND(COALESCE(vmgc.total_amount, 0) * 100) AS BIGINT) AS gap_cor_5120,
    CAST(ROUND(COALESCE(vmgae.total_amount, 0) * 100) AS BIGINT) AS gap_advance_5120a,
    CAST(ROUND((COALESCE(vmgc.total_amount, 0) + COALESCE(vmgae.total_amount, 0)) * 100) AS BIGINT) AS gap_total_cost,
    CAST(ROUND(COALESCE(vmtfe.total_amount, 0) * 100) AS BIGINT) AS titling_fees_5141,
    CAST(ROUND((COALESCE(vmvc.total_amount, 0) + COALESCE(vmvae.total_amount, 0) + COALESCE(vmgc.total_amount, 0) + COALESCE(vmgae.total_amount, 0) + COALESCE(vmtfe.total_amount, 0)) * 100) AS BIGINT) AS cor_total,
    CAST(ROUND((
      -- Total Revenue minus Total Expenses
      (COALESCE(vmr.total_amount, 0) + aa.allocated_reserve_bonus + aa.allocated_reserve_chargeback +
       COALESCE(vmvr.total_amount, 0) + aa.allocated_vsc_advance + COALESCE(vmvb.total_amount, 0) + aa.allocated_vsc_cost + aa.allocated_vsc_chargeback +
       COALESCE(vmgr.total_amount, 0) + aa.allocated_gap_advance + COALESCE(vmgvb.total_amount, 0) + aa.allocated_gap_cost + aa.allocated_gap_chargeback +
       COALESCE(vmdf.total_amount, 0) + aa.allocated_doc_chargeback + COALESCE(vmtf.total_amount, 0) + aa.allocated_titling_fees +
       aa.allocated_rebates) -
      (aa.allocated_funding_clerks + aa.allocated_commission + COALESCE(vmpv.total_amount, 0) + COALESCE(vmstv.total_amount, 0) + 
       COALESCE(vmrv.total_amount, 0) + COALESCE(vmce.total_amount, 0) + COALESCE(vmp.total_amount, 0) + COALESCE(vmtfe.total_amount, 0) + 
       COALESCE(vmbbf.total_amount, 0) + COALESCE(vmvc.total_amount, 0) + COALESCE(vmvae.total_amount, 0) + 
       COALESCE(vmgc.total_amount, 0) + COALESCE(vmgae.total_amount, 0) + COALESCE(vmra.total_amount, 0))
    ) * 100) AS BIGINT) AS gross_profit,
    CAST(0 AS BIGINT) AS gross_margin, -- Calculate as percentage later if needed
    CAST(ROUND(COALESCE(vmra.total_amount, 0) * 100) AS BIGINT) AS repo,
    'netsuite' AS deal_source,
    
    'bronze.ns.salesinvoiced' as _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
    
  FROM allocated_amounts aa
  -- Join VIN-matching amounts
  LEFT JOIN vin_matching_reserves vmr ON aa.vin = vmr.vin
  LEFT JOIN vin_matching_vsc_revenue vmvr ON aa.vin = vmvr.vin
  LEFT JOIN vin_matching_vsc_bonus vmvb ON aa.vin = vmvb.vin
  LEFT JOIN vin_matching_gap_revenue vmgr ON aa.vin = vmgr.vin
  LEFT JOIN vin_matching_gap_volume_bonus vmgvb ON aa.vin = vmgvb.vin
  LEFT JOIN vin_matching_doc_fees vmdf ON aa.vin = vmdf.vin
  LEFT JOIN vin_matching_titling_fees vmtf ON aa.vin = vmtf.vin
  LEFT JOIN vin_matching_payoff_variance vmpv ON aa.vin = vmpv.vin
  LEFT JOIN vin_matching_sales_tax_variance vmstv ON aa.vin = vmstv.vin
  LEFT JOIN vin_matching_registration_variance vmrv ON aa.vin = vmrv.vin
  LEFT JOIN vin_matching_customer_experience vmce ON aa.vin = vmce.vin
  LEFT JOIN vin_matching_penalties vmp ON aa.vin = vmp.vin
  LEFT JOIN vin_matching_titling_fees_expense vmtfe ON aa.vin = vmtfe.vin
  LEFT JOIN vin_matching_bank_buyout_fees vmbbf ON aa.vin = vmbbf.vin
  LEFT JOIN vin_matching_vsc_cor vmvc ON aa.vin = vmvc.vin
  LEFT JOIN vin_matching_vsc_advance_expense vmvae ON aa.vin = vmvae.vin
  LEFT JOIN vin_matching_gap_cor vmgc ON aa.vin = vmgc.vin
  LEFT JOIN vin_matching_gap_advance_expense vmgae ON aa.vin = vmgae.vin
  LEFT JOIN vin_matching_repo_amount vmra ON aa.vin = vmra.vin

) AS source
ON target.deal_key = source.deal_key AND target.vin = source.vin

WHEN MATCHED THEN
  UPDATE SET
    target.netsuite_posting_date_key = source.netsuite_posting_date_key,
    target.netsuite_posting_time_key = source.netsuite_posting_time_key,
    target.revenue_recognition_date_key = source.revenue_recognition_date_key,
    target.revenue_recognition_time_key = source.revenue_recognition_time_key,
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
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    deal_key, netsuite_posting_date_key, netsuite_posting_time_key, revenue_recognition_date_key, revenue_recognition_time_key, vin, month, year,
    rev_reserve_4105, reserve_bonus_rev_4106, reserve_chargeback_rev_4107, reserve_total_rev,
    vsc_rev_4110, vsc_advance_rev_4110a, vsc_bonus_rev_4110b, vsc_cost_rev_4110c, vsc_chargeback_rev_4111, vsc_total_rev,
    gap_rev_4120, gap_advance_rev_4120a, gap_volume_bonus_rev_4120b, gap_cost_rev_4120c, gap_chargeback_rev_4121, gap_total_rev,
    doc_fees_rev_4130, doc_fees_chargeback_rev_4130c, titling_fees_rev_4141, doc_title_total_rev,
    rebates_discounts_4190, total_revenue,
    funding_clerks_5301, commission_5302, sales_guarantee_5303, ic_payoff_team_5304, outbound_commission_5305,
    title_clerks_5320, direct_emp_benefits_5330, direct_payroll_tax_5340, direct_people_cost,
    payoff_variance_5400, sales_tax_variance_5401, registration_variance_5402, customer_experience_5403, penalties_5404,
    payoff_variance_total, postage_5510, bank_buyout_fees_5520, other_cor_total,
    vsc_cor_5110, vsc_advance_5110a, vsc_total_cost, gap_cor_5120, gap_advance_5120a, gap_total_cost,
    titling_fees_5141, cor_total, gross_profit, gross_margin, repo, deal_source,
    _source_table, _load_timestamp
  )
  VALUES (
    source.deal_key, source.netsuite_posting_date_key, source.netsuite_posting_time_key, source.revenue_recognition_date_key, source.revenue_recognition_time_key, source.vin, source.month, source.year,
    source.rev_reserve_4105, source.reserve_bonus_rev_4106, source.reserve_chargeback_rev_4107, source.reserve_total_rev,
    source.vsc_rev_4110, source.vsc_advance_rev_4110a, source.vsc_bonus_rev_4110b, source.vsc_cost_rev_4110c, source.vsc_chargeback_rev_4111, source.vsc_total_rev,
    source.gap_rev_4120, source.gap_advance_rev_4120a, source.gap_volume_bonus_rev_4120b, source.gap_cost_rev_4120c, source.gap_chargeback_rev_4121, source.gap_total_rev,
    source.doc_fees_rev_4130, source.doc_fees_chargeback_rev_4130c, source.titling_fees_rev_4141, source.doc_title_total_rev,
    source.rebates_discounts_4190, source.total_revenue,
    source.funding_clerks_5301, source.commission_5302, source.sales_guarantee_5303, source.ic_payoff_team_5304, source.outbound_commission_5305,
    source.title_clerks_5320, source.direct_emp_benefits_5330, source.direct_payroll_tax_5340, source.direct_people_cost,
    source.payoff_variance_5400, source.sales_tax_variance_5401, source.registration_variance_5402, source.customer_experience_5403, source.penalties_5404,
    source.payoff_variance_total, source.postage_5510, source.bank_buyout_fees_5520, source.other_cor_total,
    source.vsc_cor_5110, source.vsc_advance_5110a, source.vsc_total_cost, source.gap_cor_5120, source.gap_advance_5120a, source.gap_total_cost,
    source.titling_fees_5141, source.cor_total, source.gross_profit, source.gross_margin, source.repo, source.deal_source,
    source._source_table, source._load_timestamp
  ); 