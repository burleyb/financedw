-- models/silver/fact/fact_deal_netsuite.sql
-- Silver layer fact table for NetSuite related deal aggregates
-- Rewritten to mirror bigdealnscreation.py logic exactly using CTEs

DROP TABLE IF EXISTS silver.finance.fact_deal_netsuite;

CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_netsuite (
  -- Keys
  deal_key STRING NOT NULL,
  netsuite_posting_date_key BIGINT,
  netsuite_posting_time_key BIGINT,
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
  
  -- Flags
  repo BOOLEAN,
  
  -- Source information
  deal_source STRING,

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer fact table for NetSuite financial aggregates - mirrors bigdealnscreation.py logic'
PARTITIONED BY (year, month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Main data processing logic using CTEs to mirror Python script
INSERT OVERWRITE silver.finance.fact_deal_netsuite
WITH 
-- Step 1: Per-VIN Selected Accounts (mirrors PerVinSelectedAccounts_df)
GetVins AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        MAX(t.trandate) AS ns_date
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

-- Revenue accounts from salesinvoiced table
Reserves AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 236
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

DocFee AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 239
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

TitlingFee AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 451
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

RevVSC AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 237
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

VSCBonus AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 479
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

GAPrev AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 238
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

GAPbonus AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 480
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

-- Expense accounts from transactionline table
PayoffVariance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 532
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

SalesTaxVariance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 533
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

RegistrationVariance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 534
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

CustomerExperience AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 538
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

Penalties AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 539
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

TitlingFeeCor AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 452
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

BankBuyoutFees AS (
    SELECT 
        t.custbody_leaseend_vinno as vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 447
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND LENGTH(t.custbody_leaseend_vinno) = 17
    GROUP BY t.custbody_leaseend_vinno
),

CORvsc AS (
    SELECT 
        t.custbody_leaseend_vinno as vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 267
        AND t.abbrevtype != 'PURCHORD'
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND LENGTH(t.custbody_leaseend_vinno) = 17
    GROUP BY t.custbody_leaseend_vinno
),

CORVSCadvance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 498
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

CORgap AS (
    SELECT 
        t.custbody_leaseend_vinno as vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 269
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND LENGTH(t.custbody_leaseend_vinno) = 17
    GROUP BY t.custbody_leaseend_vinno
),

CORadvance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 499
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

Repo AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 551
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),

-- Step 2: Combine per-VIN accounts (mirrors first part of Python script)
PerVinSelectedAccounts AS (
    SELECT
        GetVins.ns_date,
        MONTH(GetVins.ns_date) AS month,
        YEAR(GetVins.ns_date) AS year,
        GetVins.vins,
        COALESCE(r.total_amount, 0) as rev_reserve_4105,
        COALESCE(df.total_amount, 0) as doc_fees_rev_4130,
        COALESCE(tf.total_amount, 0) as titling_fees_rev_4141,
        COALESCE(rv.total_amount, 0) as vsc_rev_4110,
        COALESCE(vb.total_amount, 0) as vsc_volume_bonus_rev_4110b,
        COALESCE(gr.total_amount, 0) as gap_rev_4120,
        COALESCE(gb.total_amount, 0) as gap_volume_bonus_rev_4120b,
        COALESCE(pov.total_amount, 0) as payoff_variance_5400,
        COALESCE(stv.total_amount, 0) as sales_tax_variance_5401,
        COALESCE(rvar.total_amount, 0) as registration_variance_5402,
        COALESCE(ce.total_amount, 0) as customer_experience_5403,
        COALESCE(p.total_amount, 0) as penalties_5404,
        COALESCE(tfc.total_amount, 0) as titling_fees_5141,
        COALESCE(bbf.total_amount, 0) as bank_buyout_fees_5520,
        COALESCE(cvsc.total_amount, 0) as vsc_cor_5110,
        COALESCE(cvsca.total_amount, 0) as vsc_advance_5110a,
        COALESCE(cg.total_amount, 0) as gap_cor_5120,
        COALESCE(ca.total_amount, 0) as gap_advance_5120a,
        COALESCE(repo.total_amount, 0) as repo_amount
    FROM GetVins
    LEFT JOIN Reserves AS r ON GetVins.vins = r.vins
    LEFT JOIN DocFee AS df ON GetVins.vins = df.vins
    LEFT JOIN TitlingFee tf ON GetVins.vins = tf.vins
    LEFT JOIN RevVSC rv ON GetVins.vins = rv.vins
    LEFT JOIN VSCBonus vb ON GetVins.vins = vb.vins
    LEFT JOIN GAPrev gr ON GetVins.vins = gr.vins
    LEFT JOIN GAPbonus gb ON GetVins.vins = gb.vins
    LEFT JOIN PayoffVariance pov ON GetVins.vins = pov.vins
    LEFT JOIN SalesTaxVariance stv ON GetVins.vins = stv.vins
    LEFT JOIN RegistrationVariance rvar ON GetVins.vins = rvar.vins
    LEFT JOIN CustomerExperience ce ON GetVins.vins = ce.vins
    LEFT JOIN Penalties p ON GetVins.vins = p.vins
    LEFT JOIN TitlingFeeCor tfc ON GetVins.vins = tfc.vins
    LEFT JOIN BankBuyoutFees bbf ON GetVins.vins = bbf.vins
    LEFT JOIN CORvsc cvsc ON GetVins.vins = cvsc.vins
    LEFT JOIN CORVSCadvance cvsca ON GetVins.vins = cvsca.vins
    LEFT JOIN CORgap cg ON GetVins.vins = cg.vins
    LEFT JOIN CORadvance ca ON GetVins.vins = ca.vins
    LEFT JOIN Repo repo ON GetVins.vins = repo.vins    
    WHERE GetVins.vins IS NOT NULL
),

-- Step 3: Get completion dates from deals (mirrors completion_date_df)
CompletionDates AS (
    SELECT 
        d.completion_date_utc, 
        UPPER(cars.vin) as vin 
    FROM bronze.leaseend_db_public.cars 
    LEFT JOIN bronze.leaseend_db_public.deals d ON cars.deal_id = d.id
    WHERE d.completion_date_utc IS NOT NULL
),

-- Step 4: Override NS date with completion date (mirrors Python logic)
PerVinWithCompletionDates AS (
    SELECT 
        pv.*,
        COALESCE(cd.completion_date_utc, pv.ns_date) as final_ns_date
    FROM PerVinSelectedAccounts pv
    LEFT JOIN CompletionDates cd ON UPPER(pv.vins) = UPPER(cd.vin)
),

-- Step 4.5: Map VINs to Deal IDs
VinToDealMapping AS (
    SELECT DISTINCT
        UPPER(c.vin) as vin_upper,
        CAST(d.id AS STRING) as deal_id
    FROM bronze.leaseend_db_public.cars c
    INNER JOIN bronze.leaseend_db_public.deals d ON c.deal_id = d.id
    WHERE c.vin IS NOT NULL 
      AND d.id IS NOT NULL
      AND LENGTH(c.vin) = 17
      AND (d._fivetran_deleted = FALSE OR d._fivetran_deleted IS NULL)
),

-- Step 5: Missing VIN Accounts (mirrors MissingVinAccountMissingVins_df)
-- This handles all transactions with missing or invalid VINs, aggregated by month/year
MissingVinReserves AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 236
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(so.trandate), YEAR(so.trandate)
),

MissingVinDocFee AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 239
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(so.trandate), YEAR(so.trandate)
),

MissingVinTitlingFee AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 451
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(so.trandate), YEAR(so.trandate)
),

MissingVinRevVSC AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 237
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(so.trandate), YEAR(so.trandate)
),

MissingVinVSCBonus AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 479
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(so.trandate), YEAR(so.trandate)
),

MissingVinGAPrev AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 238
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(so.trandate), YEAR(so.trandate)
),

MissingVinGAPbonus AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE account = 480
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(so.trandate), YEAR(so.trandate)
),

-- Missing VIN expense accounts
MissingVinPayoffVariance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 532
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinSalesTaxVariance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 533
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinRegistrationVariance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 534
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinCustomerExperience AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 538
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinPenalties AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 539
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinTitlingFeeCor AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 452
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinBankBuyoutFees AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 447
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinCORvsc AS (
    SELECT 
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount       
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 267
        AND t.abbrevtype != 'PURCHORD'
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (t.custbody_leaseend_vinno IS NULL OR LENGTH(t.custbody_leaseend_vinno) != 17)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinCORVSCadvance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 498
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinCORgap AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 269
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinCORadvance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 499
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

MissingVinRepo AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE tl.expenseaccount = 551
        AND (LENGTH(t.custbody_leaseend_vinno) != 17
             OR TRIM(t.custbody_leaseend_vinno) = ''
             OR t.custbody_leaseend_vinno IS NULL)
    GROUP BY MONTH(t.trandate), YEAR(t.trandate)
),

-- Step 6: Combine missing VIN accounts by month/year
MissingVinAccounts AS (
    SELECT
        mv.month,
        mv.year,
        CONCAT(mv.month, '-', mv.year, '-Missing Vin') AS vins,
        COALESCE(mvr.total_amount, 0) as rev_reserve_4105,
        COALESCE(mvdf.total_amount, 0) as doc_fees_rev_4130,
        COALESCE(mvtf.total_amount, 0) as titling_fees_rev_4141,
        COALESCE(mvrv.total_amount, 0) as vsc_rev_4110,
        COALESCE(mvvb.total_amount, 0) as vsc_volume_bonus_rev_4110b,
        COALESCE(mvgr.total_amount, 0) as gap_rev_4120,
        COALESCE(mvgb.total_amount, 0) as gap_volume_bonus_rev_4120b,
        COALESCE(mvpov.total_amount, 0) as payoff_variance_5400,
        COALESCE(mvstv.total_amount, 0) as sales_tax_variance_5401,
        COALESCE(mvrvar.total_amount, 0) as registration_variance_5402,
        COALESCE(mvce.total_amount, 0) as customer_experience_5403,
        COALESCE(mvp.total_amount, 0) as penalties_5404,
        COALESCE(mvtfc.total_amount, 0) as titling_fees_5141,
        COALESCE(mvbbf.total_amount, 0) as bank_buyout_fees_5520,
        COALESCE(mvcvsc.total_amount, 0) as vsc_cor_5110,
        COALESCE(mvcvsca.total_amount, 0) as vsc_advance_5110a,
        COALESCE(mvcg.total_amount, 0) as gap_cor_5120,
        COALESCE(mvca.total_amount, 0) as gap_advance_5120a,
        COALESCE(mvrepo.total_amount, 0) as repo_amount
    FROM (
        SELECT DISTINCT month, year 
        FROM MissingVinReserves
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinDocFee
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinTitlingFee
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinRevVSC
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinVSCBonus
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinGAPrev
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinGAPbonus
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinPayoffVariance
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinSalesTaxVariance
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinRegistrationVariance
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinCustomerExperience
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinPenalties
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinTitlingFeeCor
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinBankBuyoutFees
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinCORvsc
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinCORVSCadvance
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinCORgap
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinCORadvance
        UNION
        SELECT DISTINCT month, year 
        FROM MissingVinRepo
    ) mv
    LEFT JOIN MissingVinReserves mvr ON mv.month = mvr.month AND mv.year = mvr.year
    LEFT JOIN MissingVinDocFee mvdf ON mv.month = mvdf.month AND mv.year = mvdf.year
    LEFT JOIN MissingVinTitlingFee mvtf ON mv.month = mvtf.month AND mv.year = mvtf.year
    LEFT JOIN MissingVinRevVSC mvrv ON mv.month = mvrv.month AND mv.year = mvrv.year
    LEFT JOIN MissingVinVSCBonus mvvb ON mv.month = mvvb.month AND mv.year = mvvb.year
    LEFT JOIN MissingVinGAPrev mvgr ON mv.month = mvgr.month AND mv.year = mvgr.year
    LEFT JOIN MissingVinGAPbonus mvgb ON mv.month = mvgb.month AND mv.year = mvgb.year
    LEFT JOIN MissingVinPayoffVariance mvpov ON mv.month = mvpov.month AND mv.year = mvpov.year
    LEFT JOIN MissingVinSalesTaxVariance mvstv ON mv.month = mvstv.month AND mv.year = mvstv.year
    LEFT JOIN MissingVinRegistrationVariance mvrvar ON mv.month = mvrvar.month AND mv.year = mvrvar.year
    LEFT JOIN MissingVinCustomerExperience mvce ON mv.month = mvce.month AND mv.year = mvce.year
    LEFT JOIN MissingVinPenalties mvp ON mv.month = mvp.month AND mv.year = mvp.year
    LEFT JOIN MissingVinTitlingFeeCor mvtfc ON mv.month = mvtfc.month AND mv.year = mvtfc.year
    LEFT JOIN MissingVinBankBuyoutFees mvbbf ON mv.month = mvbbf.month AND mv.year = mvbbf.year
    LEFT JOIN MissingVinCORvsc mvcvsc ON mv.month = mvcvsc.month AND mv.year = mvcvsc.year
    LEFT JOIN MissingVinCORVSCadvance mvcvsca ON mv.month = mvcvsca.month AND mv.year = mvcvsca.year
    LEFT JOIN MissingVinCORgap mvcg ON mv.month = mvcg.month AND mv.year = mvcg.year
    LEFT JOIN MissingVinCORadvance mvca ON mv.month = mvca.month AND mv.year = mvca.year
    LEFT JOIN MissingVinRepo mvrepo ON mv.month = mvrepo.month AND mv.year = mvrepo.year
),

-- Step 7: Union all data sources and final output
AllAccounts AS (
    SELECT
        vins,
        final_ns_date,
        MONTH(final_ns_date) AS month,
        YEAR(final_ns_date) AS year,
        rev_reserve_4105,
        doc_fees_rev_4130,
        titling_fees_rev_4141,
        vsc_rev_4110,
        vsc_volume_bonus_rev_4110b,
        gap_rev_4120,
        gap_volume_bonus_rev_4120b,
        payoff_variance_5400,
        sales_tax_variance_5401,
        registration_variance_5402,
        customer_experience_5403,
        penalties_5404,
        titling_fees_5141,
        bank_buyout_fees_5520,
        vsc_cor_5110,
        vsc_advance_5110a,
        gap_cor_5120,
        gap_advance_5120a,
        repo_amount,
        vtd.deal_id
    FROM PerVinWithCompletionDates pv
    LEFT JOIN VinToDealMapping vtd ON UPPER(pv.vins) = vtd.vin_upper
    
    UNION ALL
    
    SELECT
        vins,
        CAST(CONCAT(year, '-', LPAD(month, 2, '0'), '-01') AS TIMESTAMP) as final_ns_date,
        month,
        year,
        rev_reserve_4105,
        doc_fees_rev_4130,
        titling_fees_rev_4141,
        vsc_rev_4110,
        vsc_volume_bonus_rev_4110b,
        gap_rev_4120,
        gap_volume_bonus_rev_4120b,
        payoff_variance_5400,
        sales_tax_variance_5401,
        registration_variance_5402,
        customer_experience_5403,
        penalties_5404,
        titling_fees_5141,
        bank_buyout_fees_5520,
        vsc_cor_5110,
        vsc_advance_5110a,
        gap_cor_5120,
        gap_advance_5120a,
        repo_amount,
        NULL as deal_id  -- Missing VIN accounts don't have deal IDs
    FROM MissingVinAccounts
)

-- Final SELECT with all calculations
SELECT
    COALESCE(deal_id, CONCAT('missing_vin_', year, '_', LPAD(month, 2, '0'))) AS deal_key,
    COALESCE(CAST(DATE_FORMAT(final_ns_date, 'yyyyMMdd') AS BIGINT), CAST(CONCAT(year, LPAD(month, 2, '0'), '01') AS BIGINT)) AS netsuite_posting_date_key,
    COALESCE(CAST(DATE_FORMAT(final_ns_date, 'HHmmss') AS BIGINT), CAST('000000' AS BIGINT)) AS netsuite_posting_time_key,
    UPPER(vins) AS vin,
    month,
    year,
    CAST(ROUND(rev_reserve_4105 * 100) AS BIGINT) AS rev_reserve_4105,
    CAST(0 AS BIGINT) AS reserve_bonus_rev_4106,
    CAST(0 AS BIGINT) AS reserve_chargeback_rev_4107,
    CAST(ROUND(rev_reserve_4105 * 100) AS BIGINT) AS reserve_total_rev,
    CAST(ROUND(vsc_rev_4110 * 100) AS BIGINT) AS vsc_rev_4110,
    CAST(0 AS BIGINT) AS vsc_advance_rev_4110a,
    CAST(ROUND(vsc_volume_bonus_rev_4110b * 100) AS BIGINT) AS vsc_volume_bonus_rev_4110b,
    CAST(0 AS BIGINT) AS vsc_cost_rev_4110c,
    CAST(0 AS BIGINT) AS vsc_chargeback_rev_4111,
    CAST(ROUND((vsc_rev_4110 + vsc_volume_bonus_rev_4110b) * 100) AS BIGINT) AS vsc_total_rev,
    CAST(ROUND(gap_rev_4120 * 100) AS BIGINT) AS gap_rev_4120,
    CAST(0 AS BIGINT) AS gap_advance_rev_4120a,
    CAST(ROUND(gap_volume_bonus_rev_4120b * 100) AS BIGINT) AS gap_volume_bonus_rev_4120b,
    CAST(0 AS BIGINT) AS gap_cost_rev_4120c,
    CAST(0 AS BIGINT) AS gap_chargeback_rev_4121,
    CAST(ROUND((gap_rev_4120 + gap_volume_bonus_rev_4120b) * 100) AS BIGINT) AS gap_total_rev,
    CAST(ROUND(doc_fees_rev_4130 * 100) AS BIGINT) AS doc_fees_rev_4130,
    CAST(0 AS BIGINT) AS doc_fees_chargeback_rev_4130c,
    CAST(ROUND(titling_fees_rev_4141 * 100) AS BIGINT) AS titling_fees_rev_4141,
    CAST(ROUND((doc_fees_rev_4130 + titling_fees_rev_4141) * 100) AS BIGINT) AS doc_title_total_rev,
    CAST(0 AS BIGINT) AS rebates_discounts_4190,
    CAST(ROUND((rev_reserve_4105 + vsc_rev_4110 + vsc_volume_bonus_rev_4110b + gap_rev_4120 + gap_volume_bonus_rev_4120b + doc_fees_rev_4130 + titling_fees_rev_4141) * 100) AS BIGINT) AS total_revenue,
    CAST(0 AS BIGINT) AS funding_clerks_5301,
    CAST(0 AS BIGINT) AS commission_5302,
    CAST(0 AS BIGINT) AS sales_guarantee_5303,
    CAST(0 AS BIGINT) AS ic_payoff_team_5304,
    CAST(0 AS BIGINT) AS outbound_commission_5305,
    CAST(0 AS BIGINT) AS title_clerks_5320,
    CAST(0 AS BIGINT) AS direct_emp_benefits_5330,
    CAST(0 AS BIGINT) AS direct_payroll_tax_5340,
    CAST(0 AS BIGINT) AS direct_people_cost,
    CAST(ROUND(payoff_variance_5400 * 100) AS BIGINT) AS payoff_variance_5400,
    CAST(ROUND(sales_tax_variance_5401 * 100) AS BIGINT) AS sales_tax_variance_5401,
    CAST(ROUND(registration_variance_5402 * 100) AS BIGINT) AS registration_variance_5402,
    CAST(ROUND(customer_experience_5403 * 100) AS BIGINT) AS customer_experience_5403,
    CAST(ROUND(penalties_5404 * 100) AS BIGINT) AS penalties_5404,
    CAST(ROUND((payoff_variance_5400 + sales_tax_variance_5401 + registration_variance_5402 + customer_experience_5403 + penalties_5404) * 100) AS BIGINT) AS payoff_variance_total,
    CAST(0 AS BIGINT) AS postage_5510,
    CAST(ROUND(bank_buyout_fees_5520 * 100) AS BIGINT) AS bank_buyout_fees_5520,
    CAST(ROUND(bank_buyout_fees_5520 * 100) AS BIGINT) AS other_cor_total,
    CAST(ROUND(vsc_cor_5110 * 100) AS BIGINT) AS vsc_cor_5110,
    CAST(ROUND(vsc_advance_5110a * 100) AS BIGINT) AS vsc_advance_5110a,
    CAST(ROUND((vsc_cor_5110 + vsc_advance_5110a) * 100) AS BIGINT) AS vsc_total_cost,
    CAST(ROUND(gap_cor_5120 * 100) AS BIGINT) AS gap_cor_5120,
    CAST(ROUND(gap_advance_5120a * 100) AS BIGINT) AS gap_advance_5120a,
    CAST(ROUND((gap_cor_5120 + gap_advance_5120a) * 100) AS BIGINT) AS gap_total_cost,
    CAST(ROUND(titling_fees_5141 * 100) AS BIGINT) AS titling_fees_5141,
    CAST(ROUND((payoff_variance_5400 + sales_tax_variance_5401 + registration_variance_5402 + customer_experience_5403 + penalties_5404 + bank_buyout_fees_5520 + vsc_cor_5110 + vsc_advance_5110a + gap_cor_5120 + gap_advance_5120a + titling_fees_5141) * 100) AS BIGINT) AS cor_total,
    CAST(ROUND(((rev_reserve_4105 + vsc_rev_4110 + vsc_volume_bonus_rev_4110b + gap_rev_4120 + gap_volume_bonus_rev_4120b + doc_fees_rev_4130 + titling_fees_rev_4141) - (payoff_variance_5400 + sales_tax_variance_5401 + registration_variance_5402 + customer_experience_5403 + penalties_5404 + bank_buyout_fees_5520 + vsc_cor_5110 + vsc_advance_5110a + gap_cor_5120 + gap_advance_5120a + titling_fees_5141)) * 100) AS BIGINT) AS gross_profit,
    CAST(
        CASE 
            WHEN (rev_reserve_4105 + vsc_rev_4110 + vsc_volume_bonus_rev_4110b + gap_rev_4120 + gap_volume_bonus_rev_4120b + doc_fees_rev_4130 + titling_fees_rev_4141) = 0 THEN 0
            ELSE ROUND((((rev_reserve_4105 + vsc_rev_4110 + vsc_volume_bonus_rev_4110b + gap_rev_4120 + gap_volume_bonus_rev_4120b + doc_fees_rev_4130 + titling_fees_rev_4141) - (payoff_variance_5400 + sales_tax_variance_5401 + registration_variance_5402 + customer_experience_5403 + penalties_5404 + bank_buyout_fees_5520 + vsc_cor_5110 + vsc_advance_5110a + gap_cor_5120 + gap_advance_5120a + titling_fees_5141)) / (rev_reserve_4105 + vsc_rev_4110 + vsc_volume_bonus_rev_4110b + gap_rev_4120 + gap_volume_bonus_rev_4120b + doc_fees_rev_4130 + titling_fees_rev_4141)) * 10000)
        END AS BIGINT
    ) AS gross_margin,
    CASE WHEN repo_amount > 0 THEN TRUE ELSE FALSE END AS repo,
    CASE 
        WHEN vins LIKE '%Missing Vin%' THEN 'netsuite_missing_vin'
        ELSE 'netsuite_with_vin'
    END AS deal_source,
    'silver.finance.fact_deal_netsuite' AS _source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
FROM AllAccounts; 