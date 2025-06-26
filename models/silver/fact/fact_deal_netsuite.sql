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
  
  -- Credit Memo Flags
  has_credit_memo BOOLEAN,
  credit_memo_date_key INT, -- FK to dim_date
  credit_memo_time_key INT, -- FK to dim_time
  
  vin STRING,
  month INT,
  year INT,
  
  -- Revenue fields (stored as BIGINT cents)
  4105_rev_reserve BIGINT,
  reserve_bonus_rev_4106 BIGINT,
  reserve_chargeback_rev_4107 BIGINT,
  reserve_total_rev BIGINT,
  vsc_rev_4110 BIGINT,
  vsc_advance_rev_4110a BIGINT,
  vsc_volume_bonus_rev_4110b BIGINT,
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
  `doc_&_title_total_rev` BIGINT,
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
  vsc_total BIGINT,
  gap_cor_5120 BIGINT,
  gap_advance_5120a BIGINT,
  gap_total BIGINT,
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
        t.custbody_le_deal_id as deal_id,
        from_utc_timestamp(t.trandate, 'America/Denver') as revenue_recognition_date_mt,
        DATE_FORMAT(from_utc_timestamp(t.trandate, 'America/Denver'), 'yyyy-MM') as revenue_recognition_period,
        YEAR(from_utc_timestamp(t.trandate, 'America/Denver')) as revenue_recognition_year,
        MONTH(from_utc_timestamp(t.trandate, 'America/Denver')) as revenue_recognition_month,
        QUARTER(from_utc_timestamp(t.trandate, 'America/Denver')) as revenue_recognition_quarter,
        ROW_NUMBER() OVER (PARTITION BY t.custbody_le_deal_id ORDER BY t.trandate ASC, t.createddate ASC) as rn
    FROM bronze.ns.transaction t
    WHERE t.abbrevtype = 'SALESORD'  -- Sales orders only
        AND t.custbody_le_deal_id IS NOT NULL 
        AND t.trandate IS NOT NULL
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)  -- Approved or no approval needed
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
  
  credit_memo_vins AS (
    SELECT UPPER(t.custbody_leaseend_vinno) AS vin, MIN(DATE(t.trandate)) AS credit_memo_date
    FROM bronze.ns.transaction t
    WHERE t.abbrevtype = 'CREDITMEMO'
      AND t.custbody_leaseend_vinno IS NOT NULL
      AND LENGTH(t.custbody_leaseend_vinno) = 17
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
    GROUP BY UPPER(t.custbody_leaseend_vinno)
  ),
  
  revenue_recognition_with_vins AS (
    SELECT 
        frr.deal_id,
        frr.revenue_recognition_date_mt,
        frr.revenue_recognition_period,
        frr.revenue_recognition_year,
        frr.revenue_recognition_month,
        frr.revenue_recognition_quarter,
        dv.vin
    FROM revenue_recognition_data frr
    INNER JOIN deal_vins dv ON frr.deal_id = dv.deal_id
    LEFT JOIN credit_memo_vins cmv ON dv.vin = cmv.vin
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
  
  -- Missing VIN revenue accounts by revenue recognition period (not transaction date)
  missing_vin_reserves AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 236
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_reserve_bonus AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 474
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_reserve_chargeback AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 524
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_vsc_advance AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 546
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_vsc_cost AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 545
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_vsc_chargeback AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 544
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_gap_advance AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 547
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_gap_cost AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 548
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_gap_chargeback AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 549
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_doc_chargeback AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 517
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_rebates AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE so.account = 563
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  

  
  -- Missing VIN expense accounts by revenue recognition period (not transaction date)
  missing_vin_funding_clerks AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 528
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_commission AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 552
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_sales_guarantee AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 553
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_ic_payoff_team AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 556
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_outbound_commission AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 564
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_title_clerks AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 529
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_direct_emp_benefits AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 530
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_direct_payroll_tax AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 531
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  missing_vin_postage AS (
    SELECT
        rrwv.revenue_recognition_period,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    WHERE tl.expenseaccount = 541
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
    GROUP BY rrwv.revenue_recognition_period
  ),
  
  -- Final allocation to deals
  allocated_amounts AS (
    SELECT
        rrwv.deal_id,
        rrwv.vin,
        rrwv.revenue_recognition_date_mt as ns_date,
        rrwv.revenue_recognition_month as month,
        rrwv.revenue_recognition_year as year,
        
        -- Allocate missing VIN amounts proportionally with better NULL handling
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvr.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_reserve_bonus,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvrc.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_reserve_chargeback,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvva.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_vsc_advance,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvvc.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_vsc_cost,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvvch.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_vsc_chargeback,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvga.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_gap_advance,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvgc.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_gap_cost,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvgch.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_gap_chargeback,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvdc.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_doc_chargeback,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvreb.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_rebates,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvfc.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_funding_clerks,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvcom.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_commission,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvsg.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_sales_guarantee,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvipt.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_ic_payoff_team,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvoc.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_outbound_commission,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvtc.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_title_clerks,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvdeb.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_direct_emp_benefits,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvdpt.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_direct_payroll_tax,
        
        COALESCE(
            CASE 
                WHEN dpp.deal_count > 0 THEN mvpost.total_amount / dpp.deal_count 
                ELSE 0 
            END, 
            0
        ) as allocated_postage
        
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    LEFT JOIN missing_vin_reserve_bonus mvr ON rrwv.revenue_recognition_period = mvr.revenue_recognition_period
    LEFT JOIN missing_vin_reserve_chargeback mvrc ON rrwv.revenue_recognition_period = mvrc.revenue_recognition_period
    LEFT JOIN missing_vin_vsc_advance mvva ON rrwv.revenue_recognition_period = mvva.revenue_recognition_period
    LEFT JOIN missing_vin_vsc_cost mvvc ON rrwv.revenue_recognition_period = mvvc.revenue_recognition_period
    LEFT JOIN missing_vin_vsc_chargeback mvvch ON rrwv.revenue_recognition_period = mvvch.revenue_recognition_period
    LEFT JOIN missing_vin_gap_advance mvga ON rrwv.revenue_recognition_period = mvga.revenue_recognition_period
    LEFT JOIN missing_vin_gap_cost mvgc ON rrwv.revenue_recognition_period = mvgc.revenue_recognition_period
    LEFT JOIN missing_vin_gap_chargeback mvgch ON rrwv.revenue_recognition_period = mvgch.revenue_recognition_period
    LEFT JOIN missing_vin_doc_chargeback mvdc ON rrwv.revenue_recognition_period = mvdc.revenue_recognition_period
    LEFT JOIN missing_vin_rebates mvreb ON rrwv.revenue_recognition_period = mvreb.revenue_recognition_period
    LEFT JOIN missing_vin_funding_clerks mvfc ON rrwv.revenue_recognition_period = mvfc.revenue_recognition_period
    LEFT JOIN missing_vin_commission mvcom ON rrwv.revenue_recognition_period = mvcom.revenue_recognition_period
    LEFT JOIN missing_vin_sales_guarantee mvsg ON rrwv.revenue_recognition_period = mvsg.revenue_recognition_period
    LEFT JOIN missing_vin_ic_payoff_team mvipt ON rrwv.revenue_recognition_period = mvipt.revenue_recognition_period
    LEFT JOIN missing_vin_outbound_commission mvoc ON rrwv.revenue_recognition_period = mvoc.revenue_recognition_period
    LEFT JOIN missing_vin_title_clerks mvtc ON rrwv.revenue_recognition_period = mvtc.revenue_recognition_period
    LEFT JOIN missing_vin_direct_emp_benefits mvdeb ON rrwv.revenue_recognition_period = mvdeb.revenue_recognition_period
    LEFT JOIN missing_vin_direct_payroll_tax mvdpt ON rrwv.revenue_recognition_period = mvdpt.revenue_recognition_period
    LEFT JOIN missing_vin_postage mvpost ON rrwv.revenue_recognition_period = mvpost.revenue_recognition_period
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
    CAST(ROUND(COALESCE(ANY_VALUE(vmr.total_amount), 0) * 100) AS BIGINT) AS `4105_rev_reserve`,
    CAST(ROUND(aa.allocated_reserve_bonus * 100) AS BIGINT) AS reserve_bonus_rev_4106,
    CAST(ROUND(aa.allocated_reserve_chargeback * 100) AS BIGINT) AS reserve_chargeback_rev_4107,
    CAST(ROUND((COALESCE(ANY_VALUE(vmr.total_amount), 0) + aa.allocated_reserve_bonus + aa.allocated_reserve_chargeback) * 100) AS BIGINT) AS reserve_total_rev,
    CAST(ROUND(COALESCE(ANY_VALUE(vmvr.total_amount), 0) * 100) AS BIGINT) AS vsc_rev_4110,
    CAST(ROUND(aa.allocated_vsc_advance * 100) AS BIGINT) AS vsc_advance_rev_4110a,
    CAST(ROUND(COALESCE(ANY_VALUE(vmvb.total_amount), 0) * 100) AS BIGINT) AS vsc_volume_bonus_rev_4110b,
    CAST(ROUND(aa.allocated_vsc_cost * 100) AS BIGINT) AS vsc_cost_rev_4110c,
    CAST(ROUND(aa.allocated_vsc_chargeback * 100) AS BIGINT) AS vsc_chargeback_rev_4111,
    CAST(ROUND((COALESCE(ANY_VALUE(vmvr.total_amount), 0) + aa.allocated_vsc_advance + COALESCE(ANY_VALUE(vmvb.total_amount), 0) + aa.allocated_vsc_cost + aa.allocated_vsc_chargeback) * 100) AS BIGINT) AS vsc_total_rev,
    CAST(ROUND(COALESCE(ANY_VALUE(vmgr.total_amount), 0) * 100) AS BIGINT) AS gap_rev_4120,
    CAST(ROUND(aa.allocated_gap_advance * 100) AS BIGINT) AS gap_advance_rev_4120a,
    CAST(ROUND(COALESCE(ANY_VALUE(vmgvb.total_amount), 0) * 100) AS BIGINT) AS gap_volume_bonus_rev_4120b,
    CAST(ROUND(aa.allocated_gap_cost * 100) AS BIGINT) AS gap_cost_rev_4120c,
    CAST(ROUND(aa.allocated_gap_chargeback * 100) AS BIGINT) AS gap_chargeback_rev_4121,
    CAST(ROUND((COALESCE(ANY_VALUE(vmgr.total_amount), 0) + aa.allocated_gap_advance + COALESCE(ANY_VALUE(vmgvb.total_amount), 0) + aa.allocated_gap_cost + aa.allocated_gap_chargeback) * 100) AS BIGINT) AS gap_total_rev,
    CAST(ROUND(COALESCE(ANY_VALUE(vmdf.total_amount), 0) * 100) AS BIGINT) AS doc_fees_rev_4130,
    CAST(ROUND(aa.allocated_doc_chargeback * 100) AS BIGINT) AS doc_fees_chargeback_rev_4130c,
    CAST(ROUND(COALESCE(ANY_VALUE(vmtf.total_amount), 0) * 100) AS BIGINT) AS titling_fees_rev_4141,
    CAST(ROUND((COALESCE(ANY_VALUE(vmdf.total_amount), 0) + aa.allocated_doc_chargeback + COALESCE(ANY_VALUE(vmtf.total_amount), 0)) * 100) AS BIGINT) AS `doc_&_title_total_rev`,
    CAST(ROUND(aa.allocated_rebates * 100) AS BIGINT) AS rebates_discounts_4190,
    CAST(ROUND((
      COALESCE(ANY_VALUE(vmr.total_amount), 0) + aa.allocated_reserve_bonus + aa.allocated_reserve_chargeback +
      COALESCE(ANY_VALUE(vmvr.total_amount), 0) + aa.allocated_vsc_advance + COALESCE(ANY_VALUE(vmvb.total_amount), 0) + aa.allocated_vsc_cost + aa.allocated_vsc_chargeback +
      COALESCE(ANY_VALUE(vmgr.total_amount), 0) + aa.allocated_gap_advance + COALESCE(ANY_VALUE(vmgvb.total_amount), 0) + aa.allocated_gap_cost + aa.allocated_gap_chargeback +
      COALESCE(ANY_VALUE(vmdf.total_amount), 0) + aa.allocated_doc_chargeback + COALESCE(ANY_VALUE(vmtf.total_amount), 0) +
      aa.allocated_rebates
    ) * 100) AS BIGINT) AS total_revenue,
    
    -- Expense fields (convert to cents) - combine VIN-matching + allocated missing VIN amounts
    CAST(ROUND(aa.allocated_funding_clerks * 100) AS BIGINT) AS funding_clerks_5301,
    CAST(ROUND(aa.allocated_commission * 100) AS BIGINT) AS commission_5302,
    CAST(ROUND(aa.allocated_sales_guarantee * 100) AS BIGINT) AS sales_guarantee_5303,
    CAST(ROUND(aa.allocated_ic_payoff_team * 100) AS BIGINT) AS ic_payoff_team_5304,
    CAST(ROUND(aa.allocated_outbound_commission * 100) AS BIGINT) AS outbound_commission_5305,
    CAST(ROUND(aa.allocated_title_clerks * 100) AS BIGINT) AS title_clerks_5320,
    CAST(ROUND(aa.allocated_direct_emp_benefits * 100) AS BIGINT) AS direct_emp_benefits_5330,
    CAST(ROUND(aa.allocated_direct_payroll_tax * 100) AS BIGINT) AS direct_payroll_tax_5340,
    CAST(ROUND((aa.allocated_funding_clerks + aa.allocated_commission + aa.allocated_sales_guarantee + aa.allocated_ic_payoff_team + aa.allocated_outbound_commission + aa.allocated_title_clerks + aa.allocated_direct_emp_benefits + aa.allocated_direct_payroll_tax) * 100) AS BIGINT) AS direct_people_cost,
    CAST(ROUND(COALESCE(ANY_VALUE(vmpv.total_amount), 0) * 100) AS BIGINT) AS payoff_variance_5400,
    CAST(ROUND(COALESCE(ANY_VALUE(vmstv.total_amount), 0) * 100) AS BIGINT) AS sales_tax_variance_5401,
    CAST(ROUND(COALESCE(ANY_VALUE(vmrv.total_amount), 0) * 100) AS BIGINT) AS registration_variance_5402,
    CAST(ROUND(COALESCE(ANY_VALUE(vmce.total_amount), 0) * 100) AS BIGINT) AS customer_experience_5403,
    CAST(ROUND(COALESCE(ANY_VALUE(vmp.total_amount), 0) * 100) AS BIGINT) AS penalties_5404,
    CAST(ROUND((COALESCE(ANY_VALUE(vmpv.total_amount), 0) + COALESCE(ANY_VALUE(vmstv.total_amount), 0) + COALESCE(ANY_VALUE(vmrv.total_amount), 0) + COALESCE(ANY_VALUE(vmce.total_amount), 0) + COALESCE(ANY_VALUE(vmp.total_amount), 0)) * 100) AS BIGINT) AS payoff_variance_total,
    CAST(ROUND(aa.allocated_postage * 100) AS BIGINT) AS postage_5510,
    CAST(ROUND(COALESCE(ANY_VALUE(vmbbf.total_amount), 0) * 100) AS BIGINT) AS bank_buyout_fees_5520,
    CAST(ROUND((COALESCE(ANY_VALUE(vmbbf.total_amount), 0) + aa.allocated_postage) * 100) AS BIGINT) AS other_cor_total,
    CAST(ROUND(COALESCE(ANY_VALUE(vmvc.total_amount), 0) * 100) AS BIGINT) AS vsc_cor_5110,
    CAST(ROUND(COALESCE(ANY_VALUE(vmvae.total_amount), 0) * 100) AS BIGINT) AS vsc_advance_5110a,
    CAST(ROUND((COALESCE(ANY_VALUE(vmvc.total_amount), 0) + COALESCE(ANY_VALUE(vmvae.total_amount), 0)) * 100) AS BIGINT) AS vsc_total,
    CAST(ROUND(COALESCE(ANY_VALUE(vmgc.total_amount), 0) * 100) AS BIGINT) AS gap_cor_5120,
    CAST(ROUND(COALESCE(ANY_VALUE(vmgae.total_amount), 0) * 100) AS BIGINT) AS gap_advance_5120a,
    CAST(ROUND((COALESCE(ANY_VALUE(vmgc.total_amount), 0) + COALESCE(ANY_VALUE(vmgae.total_amount), 0)) * 100) AS BIGINT) AS gap_total,
    CAST(ROUND(COALESCE(ANY_VALUE(vmtfe.total_amount), 0) * 100) AS BIGINT) AS titling_fees_5141,
    CAST(ROUND((COALESCE(ANY_VALUE(vmvc.total_amount), 0) + COALESCE(ANY_VALUE(vmvae.total_amount), 0) + COALESCE(ANY_VALUE(vmgc.total_amount), 0) + COALESCE(ANY_VALUE(vmgae.total_amount), 0) + COALESCE(ANY_VALUE(vmtfe.total_amount), 0)) * 100) AS BIGINT) AS cor_total,
    CAST(ROUND((
      -- Total Revenue minus Total Expenses
      (COALESCE(ANY_VALUE(vmr.total_amount), 0) + aa.allocated_reserve_bonus + aa.allocated_reserve_chargeback +
       COALESCE(ANY_VALUE(vmvr.total_amount), 0) + aa.allocated_vsc_advance + COALESCE(ANY_VALUE(vmvb.total_amount), 0) + aa.allocated_vsc_cost + aa.allocated_vsc_chargeback +
       COALESCE(ANY_VALUE(vmgr.total_amount), 0) + aa.allocated_gap_advance + COALESCE(ANY_VALUE(vmgvb.total_amount), 0) + aa.allocated_gap_cost + aa.allocated_gap_chargeback +
       COALESCE(ANY_VALUE(vmdf.total_amount), 0) + aa.allocated_doc_chargeback + COALESCE(ANY_VALUE(vmtf.total_amount), 0) +
       aa.allocated_rebates) -
      (aa.allocated_funding_clerks + aa.allocated_commission + aa.allocated_sales_guarantee + aa.allocated_ic_payoff_team + 
       aa.allocated_outbound_commission + aa.allocated_title_clerks + aa.allocated_direct_emp_benefits + aa.allocated_direct_payroll_tax +
       COALESCE(ANY_VALUE(vmpv.total_amount), 0) + COALESCE(ANY_VALUE(vmstv.total_amount), 0) + COALESCE(ANY_VALUE(vmrv.total_amount), 0) + COALESCE(ANY_VALUE(vmce.total_amount), 0) + 
       COALESCE(ANY_VALUE(vmp.total_amount), 0) + aa.allocated_postage + COALESCE(ANY_VALUE(vmbbf.total_amount), 0) + COALESCE(ANY_VALUE(vmtfe.total_amount), 0) + 
       COALESCE(ANY_VALUE(vmvc.total_amount), 0) + COALESCE(ANY_VALUE(vmvae.total_amount), 0) + COALESCE(ANY_VALUE(vmgc.total_amount), 0) + COALESCE(ANY_VALUE(vmgae.total_amount), 0) + 
       COALESCE(ANY_VALUE(vmra.total_amount), 0))
    ) * 100) AS BIGINT) AS gross_profit,
    CAST(0 AS BIGINT) AS gross_margin, -- Calculate as percentage later if needed
    CAST(ROUND(COALESCE(ANY_VALUE(vmra.total_amount), 0) * 100) AS BIGINT) AS repo,
    'netsuite' AS deal_source,
    ANY_VALUE(CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END) AS has_credit_memo,
    ANY_VALUE(COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT), 0)) AS credit_memo_date_key,
    ANY_VALUE(COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0)) AS credit_memo_time_key,
    
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
  LEFT JOIN credit_memo_vins cmv ON aa.vin = cmv.vin
  LEFT JOIN silver.finance.fact_deal_netsuite_transactions sft ON CAST(aa.deal_id AS STRING) = sft.deal_key AND aa.vin = sft.vin
  GROUP BY
    aa.deal_id,
    aa.ns_date,
    aa.vin,
    aa.month,
    aa.year,
    aa.allocated_reserve_bonus,
    aa.allocated_reserve_chargeback,
    aa.allocated_vsc_advance,
    aa.allocated_vsc_cost,
    aa.allocated_vsc_chargeback,
    aa.allocated_gap_advance,
    aa.allocated_gap_cost,
    aa.allocated_gap_chargeback,
    aa.allocated_doc_chargeback,
    aa.allocated_rebates,
    aa.allocated_funding_clerks,
    aa.allocated_commission,
    aa.allocated_sales_guarantee,
    aa.allocated_ic_payoff_team,
    aa.allocated_outbound_commission,
    aa.allocated_title_clerks,
    aa.allocated_direct_emp_benefits,
    aa.allocated_direct_payroll_tax,
    aa.allocated_postage
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
    target.`4105_rev_reserve` = source.`4105_rev_reserve`,
    target.reserve_bonus_rev_4106 = source.reserve_bonus_rev_4106,
    target.reserve_chargeback_rev_4107 = source.reserve_chargeback_rev_4107,
    target.reserve_total_rev = source.reserve_total_rev,
    target.vsc_rev_4110 = source.vsc_rev_4110,
    target.vsc_advance_rev_4110a = source.vsc_advance_rev_4110a,
    target.vsc_volume_bonus_rev_4110b = source.vsc_volume_bonus_rev_4110b,
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
    target.`doc_&_title_total_rev` = source.`doc_&_title_total_rev`,
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
    target.vsc_total = source.vsc_total,
    target.gap_cor_5120 = source.gap_cor_5120,
    target.gap_advance_5120a = source.gap_advance_5120a,
    target.gap_total = source.gap_total,
    target.titling_fees_5141 = source.titling_fees_5141,
    target.cor_total = source.cor_total,
    target.gross_profit = source.gross_profit,
    target.gross_margin = source.gross_margin,
    target.repo = source.repo,
    target.deal_source = source.deal_source,
    target.has_credit_memo = source.has_credit_memo,
    target.credit_memo_date_key = source.credit_memo_date_key,
    target.credit_memo_time_key = source.credit_memo_time_key,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    deal_key, netsuite_posting_date_key, netsuite_posting_time_key, revenue_recognition_date_key, revenue_recognition_time_key, vin, month, year,
    `4105_rev_reserve`, reserve_bonus_rev_4106, reserve_chargeback_rev_4107, reserve_total_rev,
    vsc_rev_4110, vsc_advance_rev_4110a, vsc_volume_bonus_rev_4110b, vsc_cost_rev_4110c, vsc_chargeback_rev_4111, vsc_total_rev,
    gap_rev_4120, gap_advance_rev_4120a, gap_volume_bonus_rev_4120b, gap_cost_rev_4120c, gap_chargeback_rev_4121, gap_total_rev,
    doc_fees_rev_4130, doc_fees_chargeback_rev_4130c, titling_fees_rev_4141, `doc_&_title_total_rev`,
    rebates_discounts_4190, total_revenue,
    funding_clerks_5301, commission_5302, sales_guarantee_5303, ic_payoff_team_5304, outbound_commission_5305,
    title_clerks_5320, direct_emp_benefits_5330, direct_payroll_tax_5340, direct_people_cost,
    payoff_variance_5400, sales_tax_variance_5401, registration_variance_5402, customer_experience_5403, penalties_5404,
    payoff_variance_total, postage_5510, bank_buyout_fees_5520, other_cor_total,
    vsc_cor_5110, vsc_advance_5110a, vsc_total, gap_cor_5120, gap_advance_5120a, gap_total,
    titling_fees_5141, cor_total, gross_profit, gross_margin, repo, deal_source,
    has_credit_memo, credit_memo_date_key, credit_memo_time_key,
    _source_table, _load_timestamp
  )
  VALUES (
    source.deal_key, source.netsuite_posting_date_key, source.netsuite_posting_time_key, source.revenue_recognition_date_key, source.revenue_recognition_time_key, source.vin, source.month, source.year,
    source.`4105_rev_reserve`, source.reserve_bonus_rev_4106, source.reserve_chargeback_rev_4107, source.reserve_total_rev,
    source.vsc_rev_4110, source.vsc_advance_rev_4110a, source.vsc_volume_bonus_rev_4110b, source.vsc_cost_rev_4110c, source.vsc_chargeback_rev_4111, source.vsc_total_rev,
    source.gap_rev_4120, source.gap_advance_rev_4120a, source.gap_volume_bonus_rev_4120b, source.gap_cost_rev_4120c, source.gap_chargeback_rev_4121, source.gap_total_rev,
    source.doc_fees_rev_4130, source.doc_fees_chargeback_rev_4130c, source.titling_fees_rev_4141, source.`doc_&_title_total_rev`,
    source.rebates_discounts_4190, source.total_revenue,
    source.funding_clerks_5301, source.commission_5302, source.sales_guarantee_5303, source.ic_payoff_team_5304, source.outbound_commission_5305,
    source.title_clerks_5320, source.direct_emp_benefits_5330, source.direct_payroll_tax_5340, source.direct_people_cost,
    source.payoff_variance_5400, source.sales_tax_variance_5401, source.registration_variance_5402, source.customer_experience_5403, source.penalties_5404,
    source.payoff_variance_total, source.postage_5510, source.bank_buyout_fees_5520, source.other_cor_total,
    source.vsc_cor_5110, source.vsc_advance_5110a, source.vsc_total, source.gap_cor_5120, source.gap_advance_5120a, source.gap_total,
    source.titling_fees_5141, source.cor_total, source.gross_profit, source.gross_margin, source.repo, source.deal_source,
    source.has_credit_memo, source.credit_memo_date_key, source.credit_memo_time_key,
    source._source_table, source._load_timestamp
  ); 
  