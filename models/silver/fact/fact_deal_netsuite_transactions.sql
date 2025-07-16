-- models/silver/fact/fact_deal_netsuite_transactions.sql
-- Normalized NetSuite fact table with account foreign keys for flexible pivoting

-- Drop and recreate table to ensure correct schema
DROP TABLE IF EXISTS silver.finance.fact_deal_netsuite_transactions;

-- 1. Create the normalized fact table
CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_netsuite_transactions (
  transaction_key STRING NOT NULL, -- Unique key: deal_key + account_key + transaction_type
  deal_key STRING, -- Nullable to allow VIN-only transactions that can't be linked to deals
  account_key STRING NOT NULL, -- Foreign key to dim_account (account_id as string)
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
  
  -- Transaction details
  transaction_type STRING, -- 'REVENUE' or 'EXPENSE'
  transaction_category STRING, -- 'VSC', 'GAP', 'RESERVE', 'DOC_FEES', etc.
  transaction_subcategory STRING, -- 'ADVANCE', 'BONUS', 'CHARGEBACK', 'BASE', etc.
  amount_cents BIGINT, -- Amount in cents
  amount_dollars DECIMAL(15,2), -- Amount in dollars for easier querying
  
  -- Allocation method (for missing VIN scenarios)
  allocation_method STRING, -- 'VIN_MATCH', 'PERIOD_ALLOCATION', 'DIRECT'
  allocation_factor DECIMAL(10,6), -- Factor used for period allocation
  
  -- Driver counting flag for titling fees
  is_driver_count BOOLEAN,
  
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
MERGE INTO silver.finance.fact_deal_netsuite_transactions AS target
USING (
  WITH revenue_recognition_data AS (
    SELECT
        t.custbody_le_deal_id as deal_id,
        t.trandate as revenue_recognition_date_utc,
        from_utc_timestamp(t.trandate, 'America/Denver') as revenue_recognition_date_mt,
        DATE_FORMAT(from_utc_timestamp(t.trandate, 'America/Denver'), 'yyyy-MM') as revenue_recognition_period,
        YEAR(from_utc_timestamp(t.trandate, 'America/Denver')) as revenue_recognition_year,
        MONTH(from_utc_timestamp(t.trandate, 'America/Denver')) as revenue_recognition_month,
        ROW_NUMBER() OVER (PARTITION BY t.custbody_le_deal_id ORDER BY t.trandate DESC, t.createddate DESC) as rn
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
        frr.revenue_recognition_date_utc,
        frr.revenue_recognition_date_mt,
        frr.revenue_recognition_period,
        frr.revenue_recognition_year,
        frr.revenue_recognition_month,
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
  
  -- Comprehensive account mapping that captures ALL NetSuite accounts with transactions
  -- Combines explicit business mappings with automated classification based on NetSuite account types
  account_mappings AS (
    WITH all_transactional_accounts AS (
      -- Find ALL accounts that have transactions (this is the complete universe)
      SELECT DISTINCT account as account_id
      FROM bronze.ns.salesinvoiced
      WHERE account IS NOT NULL
      
      UNION
      
      SELECT DISTINCT expenseaccount as account_id
      FROM bronze.ns.transactionline
      WHERE expenseaccount IS NOT NULL
        AND expenseaccount != 0
        
      UNION
      
      -- Include accounts from transactionaccountingline (this is where many accounts appear)
      SELECT DISTINCT account as account_id  
      FROM bronze.ns.transactionaccountingline
      WHERE account IS NOT NULL
        AND account != 0
    ),
    
    business_account_overrides AS (
      -- Map accounts by account number (not internal ID) for better maintainability
      -- Based on Income Statement structure provided by user
      SELECT 
        a.id as account_id,
        bo.transaction_type,
        bo.transaction_category, 
        bo.transaction_subcategory
      FROM bronze.ns.account a
      INNER JOIN (
        SELECT * FROM VALUES
          -- 4000 SERIES - REVENUE
          ('4105', 'REVENUE', 'RESERVE', 'BASE'),
          ('4106', 'REVENUE', 'RESERVE', 'BONUS'),
          ('4107', 'REVENUE', 'RESERVE', 'CHARGEBACK'),
          
          -- 4110 - REV - VSC  
          ('4110', 'REVENUE', 'VSC', 'BASE'),
          ('4110A', 'REVENUE', 'VSC', 'ADVANCE'),
          ('4110B', 'REVENUE', 'VSC', 'VOLUME_BONUS'),
          ('4110C', 'REVENUE', 'VSC', 'COST'),
          ('4111', 'REVENUE', 'VSC', 'CHARGEBACK'),
          
          -- 4120 - REV - GAP
          ('4120', 'REVENUE', 'GAP', 'BASE'),
          ('4120A', 'REVENUE', 'GAP', 'ADVANCE'),
          ('4120B', 'REVENUE', 'GAP', 'VOLUME_BONUS'),
          ('4120C', 'REVENUE', 'GAP', 'COST'),
          ('4121', 'REVENUE', 'GAP', 'CHARGEBACK'),
          ('4125', 'REVENUE', 'GAP', 'REINSURANCE'),
          
          -- 4130 - REV - DOC FEES
          ('4130', 'REVENUE', 'DOC_FEES', 'BASE'),
          ('4130C', 'REVENUE', 'DOC_FEES', 'CHARGEBACK'),
          
          -- 4140 - REV - TITLING FEES
          ('4141', 'REVENUE', 'TITLING_FEES', 'BASE'),
          ('4142', 'REVENUE', 'TITLING_FEES', 'BASE'),
          ('4190', 'REVENUE', 'OTHER_REVENUE', 'BASE'),
          
          -- 5000 SERIES - COST OF REVENUE
          -- 5110/5120 - VSC/GAP COR
          ('5110', 'COST_OF_REVENUE', 'VSC', 'COST'),
          ('5110A', 'COST_OF_REVENUE', 'VSC', 'ADVANCE'),
          ('5120', 'COST_OF_REVENUE', 'GAP', 'COST'),
          ('5120A', 'COST_OF_REVENUE', 'GAP', 'ADVANCE'),
          
          -- 5300 - DIRECT PEOPLE COST
          ('5301', 'COST_OF_REVENUE', 'DIRECT_PEOPLE_COST', 'FUNDING_CLERKS'),
          ('5304', 'COST_OF_REVENUE', 'DIRECT_PEOPLE_COST', 'IC_PAYOFF_TEAM'),
          ('5305', 'COST_OF_REVENUE', 'DIRECT_PEOPLE_COST', 'OUTBOUND_COMMISSION'),
          ('5320', 'COST_OF_REVENUE', 'DIRECT_PEOPLE_COST', 'TITLE_CLERKS'),
          ('5330', 'COST_OF_REVENUE', 'DIRECT_PEOPLE_COST', 'EMP_BENEFITS'),
          ('5340', 'COST_OF_REVENUE', 'DIRECT_PEOPLE_COST', 'PAYROLL_TAX'),
          
          -- 5400 - PAYOFF EXPENSE
          ('5400', 'COST_OF_REVENUE', 'PAYOFF_EXPENSE', 'PAYOFF'),
          ('5401', 'COST_OF_REVENUE', 'PAYOFF_EXPENSE', 'SALES_TAX'),
          ('5402', 'COST_OF_REVENUE', 'PAYOFF_EXPENSE', 'REGISTRATION'),
          ('5403', 'COST_OF_REVENUE', 'PAYOFF_EXPENSE', 'CUSTOMER_EXPERIENCE'),
          ('5404', 'COST_OF_REVENUE', 'PAYOFF_EXPENSE', 'PENALTIES'),
          ('5520', 'COST_OF_REVENUE', 'PAYOFF_EXPENSE', 'BANK_BUYOUT_FEES'),
          ('5141', 'COST_OF_REVENUE', 'PAYOFF_EXPENSE', 'TITLE_ONLY_FEES'),
          ('5530', 'COST_OF_REVENUE', 'PAYOFF_EXPENSE', 'TITLE_COR'),
          
          -- 5500 - COR - OTHER
          ('5199', 'COST_OF_REVENUE', 'OTHER_COR', 'REPO'),
          -- GA_EXPENSE overrides for Depreciation & Amortization
          ('7175', 'EXPENSE', 'GA_EXPENSE', 'DEPRECIATION'),
          ('7176', 'EXPENSE', 'GA_EXPENSE', 'AMORTIZATION'),
          ('5510', 'COST_OF_REVENUE', 'OTHER_COR', 'POSTAGE')
          
        AS t(account_number, transaction_type, transaction_category, transaction_subcategory)
      ) bo ON a.acctnumber = bo.account_number
      WHERE a._fivetran_deleted = FALSE
        AND (a.isinactive != 'T' OR a.isinactive IS NULL)
    )
    
    -- Include ALL transactional accounts with appropriate classification
    SELECT 
      ata.account_id,
      -- Use business override if exists, otherwise auto-classify based on account number ranges AND NetSuite account type
      COALESCE(
        bao.transaction_type,
        CASE 
          -- Income Statement account number classification (matches your income statement structure)
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 THEN 'REVENUE'                    -- 4000 series = Revenue
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 5000 AND 5999 THEN 'COST_OF_REVENUE'           -- 5000 series = Cost of Revenue
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 6000 AND 7999 THEN 'EXPENSE'                   -- 6000-7000 series = Operating Expenses
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 8000 AND 8999 THEN 'OTHER_EXPENSE'             -- 8000 series = Other Expenses (taxes)
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 9000 AND 9999 THEN 
            CASE 
              WHEN UPPER(a.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME') THEN 'OTHER_INCOME'  -- 9000 series income
              ELSE 'OTHER_EXPENSE'                                                                 -- 9000 series expense
            END
          -- Fallback to NetSuite account type for accounts outside standard ranges
          WHEN UPPER(a.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME', 'OTHINCOME') THEN 'REVENUE'
          WHEN UPPER(a.accttype) IN ('COGS', 'COSTOFGOODSSOLD') THEN 'COST_OF_REVENUE'
          WHEN UPPER(a.accttype) IN ('EXPENSE', 'OTHEREXPENSE', 'OTHEXPENSE') THEN 'EXPENSE'
          WHEN UPPER(a.accttype) IN ('BANK', 'ACCREC', 'INVENTORY', 'OTHCURRASSET', 'FIXEDASSET', 'ACCUMDEPRECIATION', 'OTHERASSET') THEN 'ASSET'
          WHEN UPPER(a.accttype) IN ('ACCTSPAY', 'CREDITCARD', 'OTHCURRLIAB', 'LONGTERMLIAB') THEN 'LIABILITY'
          WHEN UPPER(a.accttype) IN ('EQUITY', 'RETEARNINGS') THEN 'EQUITY'
          ELSE 'OTHER'
        END
      ) as transaction_type,
      -- Use business category if exists, otherwise auto-assign based on account number ranges
      COALESCE(
        bao.transaction_category,
        CASE 
          -- 4000 series revenue subcategorization
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4100 AND 4109 THEN 'RESERVE'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4110 AND 4119 THEN 'VSC'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4120 AND 4129 THEN 'GAP'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4130 AND 4139 THEN 'DOC_FEES'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4140 AND 4149 THEN 'TITLING_FEES'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 THEN 'GENERAL_REVENUE'
          
          -- 5000 series cost of revenue subcategorization
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 5300 AND 5399 THEN 'DIRECT_PEOPLE_COST'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 5400 AND 5499 THEN 'PAYOFF_EXPENSE'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 5500 AND 5599 THEN 'OTHER_COR'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 5000 AND 5999 THEN 'COST_OF_GOODS'
          
          -- 6000-7000 series expense subcategorization
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 6000 AND 6499 THEN 'PEOPLE_COST'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 6500 AND 6599 THEN 'MARKETING'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 7000 AND 7999 THEN 'GA_EXPENSE'
          
          -- 8000-9000 series other income/expense
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 8000 AND 8099 THEN 'INCOME_TAX'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 9000 AND 9099 AND UPPER(a.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME') THEN 'INTEREST'
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 9000 AND 9099 THEN 'NON_OPERATING'
          
          -- Fallback to NetSuite account type classification
          WHEN UPPER(a.accttype) IN ('INCOME', 'REVENUE') THEN 'GENERAL_REVENUE'
          WHEN UPPER(a.accttype) = 'OTHERINCOME' THEN 'OTHER_INCOME'
          WHEN UPPER(a.accttype) = 'EXPENSE' THEN 'GENERAL_EXPENSE'
          WHEN UPPER(a.accttype) IN ('COGS', 'COSTOFGOODSSOLD') THEN 'COST_OF_GOODS'
          WHEN UPPER(a.accttype) IN ('OTHEREXPENSE', 'OTHEXPENSE') THEN 'OTHER_EXPENSE'
          WHEN UPPER(a.accttype) = 'BANK' THEN 'CASH'
          WHEN UPPER(a.accttype) IN ('ACCREC', 'ACCOUNTSRECEIVABLE') THEN 'RECEIVABLES'
          WHEN UPPER(a.accttype) = 'INVENTORY' THEN 'INVENTORY'
          WHEN UPPER(a.accttype) IN ('OTHCURRASSET', 'OTHERCURRENTASSET') THEN 'CURRENT_ASSETS'
          WHEN UPPER(a.accttype) IN ('FIXEDASSET', 'FIXEDASSETS') THEN 'FIXED_ASSETS'
          WHEN UPPER(a.accttype) IN ('ACCTSPAY', 'ACCOUNTSPAYABLE') THEN 'PAYABLES'
          WHEN UPPER(a.accttype) = 'CREDITCARD' THEN 'CREDIT_CARD'
          WHEN UPPER(a.accttype) IN ('OTHCURRLIAB', 'OTHERCURRENTLIABILITY') THEN 'CURRENT_LIABILITIES'
          WHEN UPPER(a.accttype) IN ('LONGTERMLIAB', 'LONGTERMLIABILITY') THEN 'LONG_TERM_DEBT'
          WHEN UPPER(a.accttype) IN ('EQUITY', 'RETEARNINGS') THEN 'EQUITY'
          ELSE 'UNMAPPED'
        END
      ) as transaction_category,
      -- Use business subcategory if exists, otherwise use standard
      COALESCE(
        bao.transaction_subcategory,
        'STANDARD'
      ) as transaction_subcategory
    FROM all_transactional_accounts ata
    INNER JOIN bronze.ns.account a ON ata.account_id = a.id
    LEFT JOIN business_account_overrides bao ON ata.account_id = bao.account_id
    WHERE a._fivetran_deleted = FALSE
      AND (a.isinactive != 'T' OR a.isinactive IS NULL)
  ),
  
  -- VIN-matching transactions - USE SALESINVOICED ONLY for revenue accounts to avoid double-counting
  -- MUST have BOTH VIN AND Deal ID to be considered VIN-matching
  -- Only match EXACT 17-character VINs (no commas or multiple VINs)
  vin_matching_revenue AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        so.account,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Exclude multi-VIN transactions
        AND t.custbody_le_deal_id IS NOT NULL
        AND t.custbody_le_deal_id != 0
        AND am.transaction_type = 'REVENUE'
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND so.amount != 0
    GROUP BY UPPER(t.custbody_leaseend_vinno), so.account
  ),
  
  -- VIN-matching expenses - USE TRANSACTIONLINE for expense accounts (VIN + Deal ID)
  -- Exclude accounts 5110 and 5120 as they show $0 in income statement but large amounts via VIN-matching
  -- Only match EXACT 17-character VINs (no commas or multiple VINs)
  -- SIGN CORRECTION: Negate foreignamount to match income statement format (expenses should be negative)
  vin_matching_expenses AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        tl.expenseaccount as account,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Exclude multi-VIN transactions
        AND t.custbody_le_deal_id IS NOT NULL
        AND t.custbody_le_deal_id != 0
        AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND tl.foreignamount != 0
        AND tl.expenseaccount NOT IN (267, 269, 498, 499) -- Exclude 5110, 5120, 5110A, 5120A from VIN-matching
    GROUP BY UPPER(t.custbody_leaseend_vinno), tl.expenseaccount
  ),
  
  -- VIN-only expenses - USE TRANSACTIONLINE for expense accounts 
  -- Captures VINs that either have no deal_id OR have deal_id but don't match our VIN_MATCH criteria
  -- Also exclude accounts 5110, 5120, 5110A, 5120A from VIN-only matching
  -- Only match EXACT 17-character VINs (no commas or multiple VINs)
  -- SIGN CORRECTION: Negate foreignamount to match income statement format (expenses should be negative)
  vin_only_expenses AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        tl.expenseaccount as account,
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        YEAR(t.trandate) as transaction_year,
        MONTH(t.trandate) as transaction_month,
        t.trandate,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Exclude multi-VIN transactions
        -- VIN_ONLY: All single VINs (let final transaction logic handle deduplication)
        AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND tl.foreignamount != 0
        AND t.trandate IS NOT NULL
        AND tl.expenseaccount NOT IN (267, 269, 498, 499) -- Exclude 5110, 5120, 5110A, 5120A from VIN-only matching too
    GROUP BY UPPER(t.custbody_leaseend_vinno), tl.expenseaccount, DATE_FORMAT(t.trandate, 'yyyy-MM'), YEAR(t.trandate), MONTH(t.trandate), t.trandate
  ),
  
  -- NEW: Deal-only expenses (deal_id present but VIN missing). Captures vendor bills / journals that
  -- post to expense & COR accounts yet have no VIN recorded.
  deal_no_vin_expenses AS (
    SELECT
        t.custbody_le_deal_id       AS deal_id,
        tl.expenseaccount           AS account,
        t.trandate                  AS trandate,
        MONTH(t.trandate)           AS transaction_month,
        YEAR(t.trandate)            AS transaction_year,
        SUM(tl.foreignamount)       AS total_amount
    FROM bronze.ns.transactionline tl
    INNER JOIN bronze.ns.transaction t ON tl.transaction = t.id
    INNER JOIN account_mappings am     ON tl.expenseaccount = am.account_id
    WHERE (t.custbody_le_deal_id IS NOT NULL AND t.custbody_le_deal_id != 0)
      AND (t.custbody_leaseend_vinno IS NULL OR LENGTH(t.custbody_leaseend_vinno) != 17)
      AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
      AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
      AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND tl.foreignamount != 0
      AND tl.expenseaccount NOT IN (267, 269, 498, 499)
    GROUP BY t.custbody_le_deal_id, tl.expenseaccount, t.trandate, MONTH(t.trandate), YEAR(t.trandate)
  ),
  
  
  -- Multi-VIN transaction splitting for expenses
  multi_vin_expenses AS (
    WITH multi_vin_raw AS (
      SELECT
          t.id as transaction_id,
          t.trandate,
          t.custbody_leaseend_vinno as vin_list,
          tl.expenseaccount as account,
          -- SIGN CORRECTION: Negate foreignamount for expenses to match income statement format
          tl.foreignamount as total_amount,
          am.transaction_type,
          am.transaction_category,
          am.transaction_subcategory
      FROM bronze.ns.transactionline tl
      INNER JOIN bronze.ns.transaction t ON tl.transaction = t.id
      INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
      WHERE t.custbody_leaseend_vinno IS NOT NULL
          AND t.custbody_leaseend_vinno LIKE '%,%'  -- Only multi-VIN transactions
          AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
          AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
          AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
          AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND tl.foreignamount != 0
          AND tl.expenseaccount NOT IN (267, 269, 498, 499) -- Exclude 5110, 5120, 5110A, 5120A from multi-VIN too
    ),
    
    vin_splits AS (
      SELECT
          mvr.transaction_id,
          mvr.trandate,
          mvr.account,
          mvr.total_amount,
          mvr.transaction_type,
          mvr.transaction_category,
          mvr.transaction_subcategory,
          UPPER(TRIM(vin_element.col)) as individual_vin,
          SIZE(SPLIT(mvr.vin_list, ',')) as vin_count,
          vin_element.pos                                   as vin_pos,
          -- integer cents for accuracy
          CAST(ROUND(mvr.total_amount * 100) AS BIGINT)                             AS total_cents,
          CAST(ROUND(mvr.total_amount * 100) AS BIGINT) DIV SIZE(SPLIT(mvr.vin_list, ',')) AS base_split_cents,
          pmod(CAST(ROUND(mvr.total_amount * 100) AS BIGINT), SIZE(SPLIT(mvr.vin_list, ','))) AS remainder_cents,
          -- allocate the remainder (1 cent) to first `remainder_cents` VINs
          CAST(ROUND(mvr.total_amount * 100) AS BIGINT) DIV SIZE(SPLIT(mvr.vin_list, ',')) + 
          CASE WHEN vin_element.pos < pmod(CAST(ROUND(mvr.total_amount * 100) AS BIGINT), SIZE(SPLIT(mvr.vin_list, ','))) THEN 1 ELSE 0 END AS split_cents
      FROM multi_vin_raw mvr
      LATERAL VIEW posexplode(SPLIT(mvr.vin_list, ',')) vin_element AS pos, col
      WHERE LENGTH(TRIM(col)) >= 5
    )
    
    SELECT
        vs.individual_vin as vin,
        vs.account,
        vs.trandate,
        YEAR(vs.trandate) as transaction_year,
        MONTH(vs.trandate) as transaction_month,
        vs.transaction_type,
        vs.transaction_category,
        vs.transaction_subcategory,
        SUM(vs.split_cents) / 100.0 as total_amount
    FROM vin_splits vs
    GROUP BY vs.individual_vin, vs.account, vs.trandate, YEAR(vs.trandate), MONTH(vs.trandate),
             vs.transaction_type, vs.transaction_category, vs.transaction_subcategory
  ),
  
  -- Multi-VIN transaction splitting for revenue
  multi_vin_revenue AS (
    WITH multi_vin_raw AS (
      SELECT
          t.id as transaction_id,
          t.trandate,
          t.custbody_leaseend_vinno as vin_list,
          so.account,
          so.amount as total_amount,
          am.transaction_type,
          am.transaction_category,
          am.transaction_subcategory
      FROM bronze.ns.salesinvoiced so
      INNER JOIN bronze.ns.transaction t ON so.transaction = t.id
      INNER JOIN account_mappings am ON so.account = am.account_id
      WHERE t.custbody_leaseend_vinno IS NOT NULL
          AND t.custbody_leaseend_vinno LIKE '%,%'  -- Only multi-VIN transactions
          AND am.transaction_type = 'REVENUE'
          AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND so.amount != 0
    ),
    
    vin_splits AS (
      SELECT
          mvr.transaction_id,
          mvr.trandate,
          mvr.account,
          mvr.total_amount,
          mvr.transaction_type,
          mvr.transaction_category,
          mvr.transaction_subcategory,
          UPPER(TRIM(vin_element.col)) as individual_vin,
          SIZE(SPLIT(mvr.vin_list, ',')) as vin_count,
          vin_element.pos                                   as vin_pos,
          -- integer cents for accuracy
          CAST(ROUND(mvr.total_amount * 100) AS BIGINT)                             AS total_cents,
          CAST(ROUND(mvr.total_amount * 100) AS BIGINT) DIV SIZE(SPLIT(mvr.vin_list, ',')) AS base_split_cents,
          pmod(CAST(ROUND(mvr.total_amount * 100) AS BIGINT), SIZE(SPLIT(mvr.vin_list, ','))) AS remainder_cents,
          -- allocate the remainder (1 cent) to first `remainder_cents` VINs
          CAST(ROUND(mvr.total_amount * 100) AS BIGINT) DIV SIZE(SPLIT(mvr.vin_list, ',')) + 
          CASE WHEN vin_element.pos < pmod(CAST(ROUND(mvr.total_amount * 100) AS BIGINT), SIZE(SPLIT(mvr.vin_list, ','))) THEN 1 ELSE 0 END AS split_cents
      FROM multi_vin_raw mvr
      LATERAL VIEW posexplode(SPLIT(mvr.vin_list, ',')) vin_element AS pos, col
      WHERE LENGTH(TRIM(col)) >= 5
    )
    
    SELECT
        vs.individual_vin as vin,
        vs.account,
        vs.trandate,
        YEAR(vs.trandate) as transaction_year,
        MONTH(vs.trandate) as transaction_month,
        vs.transaction_type,
        vs.transaction_category,
        vs.transaction_subcategory,
        SUM(vs.split_cents) / 100.0 as total_amount
    FROM vin_splits vs
    GROUP BY vs.individual_vin, vs.account, vs.trandate, YEAR(vs.trandate), MONTH(vs.trandate),
             vs.transaction_type, vs.transaction_category, vs.transaction_subcategory
  ),
  
  -- VIN-only revenue - USE SALESINVOICED for revenue accounts
  -- Captures VINs that either have no deal_id OR have deal_id but don't match our VIN_MATCH criteria
  -- Only match EXACT 17-character VINs (no commas or multiple VINs)
  vin_only_revenue AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        so.account,
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        YEAR(t.trandate) as transaction_year,
        MONTH(t.trandate) as transaction_month,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Exclude multi-VIN transactions
        -- VIN_ONLY: All single VINs (let final transaction logic handle deduplication)
        AND am.transaction_type = 'REVENUE'
        AND t.trandate IS NOT NULL
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND so.amount != 0
    GROUP BY UPPER(t.custbody_leaseend_vinno), so.account, DATE_FORMAT(t.trandate, 'yyyy-MM'), YEAR(t.trandate), MONTH(t.trandate)
  ),
  

  
  -- Missing VIN transactions by period (for allocation) - ENHANCED with VIN lookup
  -- Try to resolve VINs first before allocating across deals
  missing_vin_revenue AS (
    WITH missing_vin_with_lookup AS (
      SELECT
          rrwv.revenue_recognition_period,
          so.account,
          so.amount,
          t.custbody_le_deal_id as deal_id,
          -- VIN lookup logic: use NetSuite VIN if valid, otherwise lookup from cars table
          COALESCE(
              CASE 
                  WHEN LENGTH(t.custbody_leaseend_vinno) = 17 
                      AND t.custbody_leaseend_vinno IS NOT NULL 
                      AND t.custbody_leaseend_vinno NOT LIKE '%,%'
                  THEN UPPER(t.custbody_leaseend_vinno)
                  ELSE NULL
              END,
              dv.vin
          ) as resolved_vin
      FROM bronze.ns.salesinvoiced AS so
      INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
      INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
      INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
      INNER JOIN account_mappings am ON so.account = am.account_id
      LEFT JOIN deal_vins dv ON t.custbody_le_deal_id = dv.deal_id
      WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
          AND t.custbody_le_deal_id IS NOT NULL
          AND am.transaction_type = 'REVENUE'
          AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND so.amount != 0
    )
    -- Only include transactions that still can't be resolved after VIN lookup
    SELECT
        revenue_recognition_period,
        account,
        SUM(amount) AS total_amount
    FROM missing_vin_with_lookup
    WHERE resolved_vin IS NULL  -- Only transactions that still have no VIN after lookup
    GROUP BY revenue_recognition_period, account
  ),
  
  -- NEW: VIN-resolved missing transactions (previously would have been allocated)
  vin_resolved_missing_revenue AS (
    WITH missing_vin_with_lookup AS (
      SELECT
          rrwv.revenue_recognition_period,
          so.account,
          so.amount,
          t.custbody_le_deal_id as deal_id,
          rrwv.vin as recognition_vin,
          -- VIN lookup logic: use NetSuite VIN if valid, otherwise lookup from cars table
          COALESCE(
              CASE 
                  WHEN LENGTH(t.custbody_leaseend_vinno) = 17 
                      AND t.custbody_leaseend_vinno IS NOT NULL 
                      AND t.custbody_leaseend_vinno NOT LIKE '%,%'
                  THEN UPPER(t.custbody_leaseend_vinno)
                  ELSE NULL
              END,
              dv.vin
          ) as resolved_vin
      FROM bronze.ns.salesinvoiced AS so
      INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
      INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
      INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
      INNER JOIN account_mappings am ON so.account = am.account_id
      LEFT JOIN deal_vins dv ON t.custbody_le_deal_id = dv.deal_id
      WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
          AND t.custbody_le_deal_id IS NOT NULL
          AND am.transaction_type = 'REVENUE'
          AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND so.amount != 0
    )
    -- Include transactions that were resolved via VIN lookup
    SELECT
        resolved_vin as vin,
        account,
        SUM(amount) AS total_amount
    FROM missing_vin_with_lookup
    WHERE resolved_vin IS NOT NULL  -- Only transactions resolved via VIN lookup
    GROUP BY resolved_vin, account
  ),
  
  missing_vin_expenses AS (
    SELECT
        rrwv.revenue_recognition_period,
        tl.expenseaccount as account,
        -- SIGN CORRECTION: Negate foreignamount for expenses to match income statement format
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
        AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND tl.foreignamount != 0
    GROUP BY rrwv.revenue_recognition_period, tl.expenseaccount
  ),
  
  -- Unallocated transactions (not tied to any deal) - SEPARATE from deal-based transactions
  unallocated_revenue AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        so.account,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    WHERE (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)
        AND t.custbody_leaseend_vinno IS NULL  -- ONLY transactions with NO VIN should be unallocated
        AND t.trandate IS NOT NULL
        AND am.transaction_type = 'REVENUE'
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND so.amount != 0
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), so.account
  ),
  
  unallocated_expenses AS (
    -- Expenses coming from transactionline (standard vendor bills, etc.)
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        tl.expenseaccount as account,
        -- SIGN CORRECTION: Negate foreignamount for expenses to match income statement format
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)
        AND t.custbody_leaseend_vinno IS NULL  -- ONLY transactions with NO VIN should be unallocated
        AND t.trandate IS NOT NULL
        AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND tl.foreignamount != 0
        AND tl.expenseaccount NOT IN (267, 269, 498, 499) -- Exclude 5110, 5120, 5110A, 5120A from unallocated too
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), tl.expenseaccount


  ),
  

  
  -- Combine VIN-matching and allocated amounts
  final_transactions_base AS (
    -- VIN-matching revenue transactions
    SELECT
      CONCAT(t.custbody_le_deal_id, '_', so.account, '_REVENUE') as transaction_key,
      CAST(t.custbody_le_deal_id AS STRING) as deal_key,
      CAST(so.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      UPPER(t.custbody_leaseend_vinno) as vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(so.amount * 100) AS BIGINT) as amount_cents,
      CAST(so.amount AS DECIMAL(15,2)) as amount_dollars,
      'VIN_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0) AS credit_memo_time_key
    FROM bronze.ns.salesinvoiced so
    INNER JOIN bronze.ns.transaction t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    LEFT JOIN credit_memo_vins cmv ON UPPER(t.custbody_leaseend_vinno) = cmv.vin
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
      AND t.custbody_leaseend_vinno IS NOT NULL
      AND t.custbody_leaseend_vinno NOT LIKE '%,%'
      AND t.custbody_le_deal_id IS NOT NULL
      AND t.custbody_le_deal_id != 0
      AND am.transaction_type = 'REVENUE'
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND so.amount != 0

    UNION ALL
 
    -- VIN-matching expense transactions
    SELECT
      CONCAT(rrwv.deal_id, '_', vme.account, '_EXPENSE') as transaction_key,
      CAST(rrwv.deal_id AS STRING) as deal_key,
      CAST(vme.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      rrwv.vin,
      rrwv.revenue_recognition_month as month,
      rrwv.revenue_recognition_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(vme.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(vme.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'VIN_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0) AS credit_memo_time_key
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN vin_matching_expenses vme ON rrwv.vin = vme.vin
    INNER JOIN account_mappings am ON vme.account = am.account_id
    LEFT JOIN credit_memo_vins cmv ON rrwv.vin = cmv.vin

    UNION ALL

    -- VIN-only revenue transactions (VIN exists but NOT already captured by VIN_MATCH)
    SELECT
      CONCAT(canonical_dv.deal_id, '_', vor.account, '_', vor.transaction_period, '_VIN_ONLY_REVENUE') as transaction_key,
      CAST(canonical_dv.deal_id AS STRING) as deal_key,
      CAST(vor.account AS STRING) as account_key,
      COALESCE(CAST(REPLACE(vor.transaction_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
      CAST(0 AS BIGINT) AS netsuite_posting_time_key,
      COALESCE(CAST(REPLACE(vor.transaction_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
      CAST(0 AS INT) AS revenue_recognition_time_key,
      vor.vin,
      vor.transaction_month as month,
      vor.transaction_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(vor.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(vor.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'VIN_ONLY_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM vin_only_revenue vor
    INNER JOIN (
        SELECT vin, MAX(deal_id) as deal_id FROM deal_vins GROUP BY vin
    ) canonical_dv ON vor.vin = canonical_dv.vin
    INNER JOIN account_mappings am ON vor.account = am.account_id
    LEFT JOIN vin_matching_revenue vmr ON vor.vin = vmr.vin AND vor.account = vmr.account
    WHERE vmr.vin IS NULL

    UNION ALL

    -- VIN-only expense transactions (VIN exists but NOT already captured by VIN_MATCH)
    SELECT
      CONCAT(canonical_dv.deal_id, '_', voe.account, '_', voe.transaction_period, '_VIN_ONLY_EXPENSE') as transaction_key,
      CAST(canonical_dv.deal_id AS STRING) as deal_key,
      CAST(voe.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(voe.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(voe.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(voe.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(voe.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      voe.vin,
      voe.transaction_month as month,
      voe.transaction_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(voe.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(voe.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'VIN_ONLY_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0) AS credit_memo_time_key
    FROM vin_only_expenses voe
    INNER JOIN (
        SELECT vin, MAX(deal_id) as deal_id FROM deal_vins GROUP BY vin
    ) canonical_dv ON voe.vin = canonical_dv.vin
    INNER JOIN account_mappings am ON voe.account = am.account_id
    LEFT JOIN credit_memo_vins cmv ON voe.vin = cmv.vin
    LEFT JOIN vin_matching_expenses vme ON voe.vin = vme.vin AND voe.account = vme.account
    WHERE vme.vin IS NULL

    UNION ALL

    -- Multi-VIN split revenue transactions (linked to deals)
    SELECT
      CONCAT(canonical_dv.deal_id, '_', mvr.account, '_', DATE_FORMAT(mvr.trandate, 'yyyyMMdd'), '_MULTI_VIN_REVENUE') as transaction_key,
      CAST(canonical_dv.deal_id AS STRING) as deal_key,
      CAST(mvr.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(mvr.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(mvr.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(mvr.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(mvr.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      mvr.vin,
      mvr.transaction_month as month,
      mvr.transaction_year as year,
      mvr.transaction_type,
      mvr.transaction_category,
      mvr.transaction_subcategory,
      CAST(ROUND(mvr.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(mvr.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'MULTI_VIN_SPLIT' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0) AS credit_memo_time_key
    FROM multi_vin_revenue mvr
    INNER JOIN (
        SELECT vin, MAX(deal_id) as deal_id FROM deal_vins GROUP BY vin
    ) canonical_dv ON mvr.vin = canonical_dv.vin
    LEFT JOIN credit_memo_vins cmv ON mvr.vin = cmv.vin

    UNION ALL

    -- Multi-VIN split expense transactions (linked to deals)
    SELECT
      CONCAT(canonical_dv.deal_id, '_', mve.account, '_', DATE_FORMAT(mve.trandate, 'yyyyMMdd'), '_MULTI_VIN_EXPENSE') as transaction_key,
      CAST(canonical_dv.deal_id AS STRING) as deal_key,
      CAST(mve.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(mve.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(mve.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(mve.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(mve.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      mve.vin,
      mve.transaction_month as month,
      mve.transaction_year as year,
      mve.transaction_type,
      mve.transaction_category,
      mve.transaction_subcategory,
      CAST(ROUND(mve.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(mve.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'MULTI_VIN_SPLIT' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0) AS credit_memo_time_key
    FROM multi_vin_expenses mve
    INNER JOIN (
        SELECT vin, MAX(deal_id) as deal_id FROM deal_vins GROUP BY vin
    ) canonical_dv ON mve.vin = canonical_dv.vin
    LEFT JOIN credit_memo_vins cmv ON mve.vin = cmv.vin

    UNION ALL

    -- Multi-VIN split revenue transactions that can't be linked to deals (orphaned VINs)
    SELECT
      CONCAT('MULTI_VIN_ORPHAN_', mvr.vin, '_', mvr.account, '_', DATE_FORMAT(mvr.trandate, 'yyyyMMdd'), '_REVENUE') as transaction_key,
      NULL as deal_key,
      CAST(mvr.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(mvr.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(mvr.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(mvr.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(mvr.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      mvr.vin,
      mvr.transaction_month as month,
      mvr.transaction_year as year,
      mvr.transaction_type,
      mvr.transaction_category,
      mvr.transaction_subcategory,
      CAST(ROUND(mvr.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(mvr.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'MULTI_VIN_ORPHAN' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM multi_vin_revenue mvr
    LEFT JOIN deal_vins dv ON mvr.vin = dv.vin
    WHERE dv.deal_id IS NULL

    UNION ALL

    -- Multi-VIN split expense transactions that can't be linked to deals (orphaned VINs)
    SELECT
      CONCAT('MULTI_VIN_ORPHAN_', mve.vin, '_', mve.account, '_', DATE_FORMAT(mve.trandate, 'yyyyMMdd'), '_EXPENSE') as transaction_key,
      NULL as deal_key,
      CAST(mve.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(mve.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(mve.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(mve.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(mve.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      mve.vin,
      mve.transaction_month as month,
      mve.transaction_year as year,
      mve.transaction_type,
      mve.transaction_category,
      mve.transaction_subcategory,
      CAST(ROUND(mve.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(mve.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'MULTI_VIN_ORPHAN' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM multi_vin_expenses mve
    LEFT JOIN deal_vins dv ON mve.vin = dv.vin
    WHERE dv.deal_id IS NULL

    UNION ALL

    -- VIN-resolved missing revenue transactions (previously would have been allocated)
    SELECT
      CONCAT(canonical_dv.deal_id, '_', vrm.account, '_VIN_RESOLVED_REVENUE') as transaction_key,
      CAST(canonical_dv.deal_id AS STRING) as deal_key,
      CAST(vrm.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      vrm.vin,
      rrwv.revenue_recognition_month as month,
      rrwv.revenue_recognition_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(vrm.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(vrm.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'VIN_LOOKUP_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0) AS credit_memo_time_key
    FROM vin_resolved_missing_revenue vrm
    INNER JOIN (
        SELECT vin, MAX(deal_id) as deal_id FROM deal_vins GROUP BY vin
    ) canonical_dv ON vrm.vin = canonical_dv.vin
    INNER JOIN revenue_recognition_with_vins rrwv ON canonical_dv.deal_id = rrwv.deal_id AND vrm.vin = rrwv.vin
    INNER JOIN account_mappings am ON vrm.account = am.account_id
    LEFT JOIN credit_memo_vins cmv ON vrm.vin = cmv.vin

    UNION ALL

    -- Allocated missing VIN revenue transactions
    SELECT
      CONCAT(rrwv.deal_id, '_', mvr.account, '_REVENUE_ALLOCATED') as transaction_key,
      CAST(rrwv.deal_id AS STRING) as deal_key,
      CAST(mvr.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      rrwv.vin,
      rrwv.revenue_recognition_month as month,
      rrwv.revenue_recognition_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND((mvr.total_amount / NULLIF(dpp.deal_count, 0)) * 100) AS BIGINT) as amount_cents,
      CAST(mvr.total_amount / NULLIF(dpp.deal_count, 0) AS DECIMAL(15,2)) as amount_dollars,
      'PERIOD_ALLOCATION' as allocation_method,
      CAST(1.0 / NULLIF(dpp.deal_count, 0) AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    INNER JOIN missing_vin_revenue mvr ON rrwv.revenue_recognition_period = mvr.revenue_recognition_period
    INNER JOIN account_mappings am ON mvr.account = am.account_id

    UNION ALL

    -- Allocated missing VIN expense transactions
    SELECT
      CONCAT(rrwv.deal_id, '_', mve.account, '_EXPENSE_ALLOCATED') as transaction_key,
      CAST(rrwv.deal_id AS STRING) as deal_key,
      CAST(mve.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      rrwv.vin,
      rrwv.revenue_recognition_month as month,
      rrwv.revenue_recognition_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND((mve.total_amount / NULLIF(dpp.deal_count, 0)) * 100) AS BIGINT) as amount_cents,
      CAST(mve.total_amount / NULLIF(dpp.deal_count, 0) AS DECIMAL(15,2)) as amount_dollars,
      'PERIOD_ALLOCATION' as allocation_method,
      CAST(1.0 / NULLIF(dpp.deal_count, 0) AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    INNER JOIN missing_vin_expenses mve ON rrwv.revenue_recognition_period = mve.revenue_recognition_period
    INNER JOIN account_mappings am ON mve.account = am.account_id

    UNION ALL

    -- Unallocated revenue transactions (not tied to any deal)
    SELECT
      CONCAT(transaction_period, '_', account, '_REVENUE_UNALLOCATED') as transaction_key,
      'UNALLOCATED' as deal_key,
      CAST(account AS STRING) as account_key,
      COALESCE(CAST(REPLACE(transaction_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
      CAST(0 AS BIGINT) AS netsuite_posting_time_key,
      COALESCE(CAST(REPLACE(transaction_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
      CAST(0 AS INT) AS revenue_recognition_time_key,
      'UNALLOCATED' as vin,
      CAST(SPLIT(transaction_period, '-')[1] AS INT) as month,
      CAST(SPLIT(transaction_period, '-')[0] AS INT) as year,
      'REVENUE' as transaction_type,
      'GENERAL_REVENUE' as transaction_category,
      'STANDARD' as transaction_subcategory,
      CAST(ROUND(total_amount * 100) AS BIGINT) as amount_cents,
      CAST(total_amount AS DECIMAL(15,2)) as amount_dollars,
      'UNALLOCATED' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM unallocated_revenue

    UNION ALL

    -- Unallocated expense transactions (not tied to any deal)
    SELECT
      CONCAT(transaction_period, '_', account, '_EXPENSE_UNALLOCATED') as transaction_key,
      'UNALLOCATED' as deal_key,
      CAST(account AS STRING) as account_key,
      COALESCE(CAST(REPLACE(transaction_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
      CAST(0 AS BIGINT) AS netsuite_posting_time_key,
      COALESCE(CAST(REPLACE(transaction_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
      CAST(0 AS INT) AS revenue_recognition_time_key,
      'UNALLOCATED' as vin,
      CAST(SPLIT(transaction_period, '-')[1] AS INT) as month,
      CAST(SPLIT(transaction_period, '-')[0] AS INT) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(total_amount * 100) AS BIGINT) as amount_cents,
      CAST(total_amount AS DECIMAL(15,2)) as amount_dollars,
      'UNALLOCATED' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM unallocated_expenses ue
    INNER JOIN account_mappings am ON ue.account = am.account_id

    UNION ALL

    -- Deal-only expense transactions (deal_id present but VIN missing)
    SELECT
      CONCAT(dnve.deal_id, '_', dnve.account, '_DEAL_ONLY_EXPENSE') as transaction_key,
      CAST(dnve.deal_id AS STRING) as deal_key,
      CAST(dnve.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(dnve.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(dnve.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(dnve.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(dnve.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      'NO_VIN' as vin,
      dnve.transaction_month as month,
      dnve.transaction_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(dnve.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(dnve.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'DEAL_ONLY' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM deal_no_vin_expenses dnve
    INNER JOIN account_mappings am ON dnve.account = am.account_id

    UNION ALL

    -- VIN-only revenue transactions with no matching deal (orphan VIN)
    SELECT
      CONCAT('VIN_ORPHAN_', vor.vin, '_', vor.account, '_', vor.transaction_period, '_REVENUE') as transaction_key,
      NULL as deal_key,
      CAST(vor.account AS STRING) as account_key,
      COALESCE(CAST(REPLACE(vor.transaction_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
      CAST(0 AS BIGINT) AS netsuite_posting_time_key,
      COALESCE(CAST(REPLACE(vor.transaction_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
      CAST(0 AS INT) AS revenue_recognition_time_key,
      vor.vin,
      vor.transaction_month as month,
      vor.transaction_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(vor.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(vor.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'VIN_ORPHAN' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key
    FROM vin_only_revenue vor
    INNER JOIN account_mappings am ON vor.account = am.account_id
    LEFT JOIN (
        SELECT vin FROM deal_vins GROUP BY vin
    ) dv ON vor.vin = dv.vin
    WHERE dv.vin IS NULL
  ),
  final_transactions AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        ORDER BY transaction_key, deal_key, vin, account_key, transaction_type, transaction_category, transaction_subcategory, amount_cents, amount_dollars, _source_table
      ) AS global_rownum
    FROM final_transactions_base
  ),
  final_transactions_with_account AS (
    SELECT
      ft.*,
      CONCAT(ft.transaction_key, '_', ft.global_rownum) AS transaction_key_unique,
      a.acctnumber,
      ROW_NUMBER() OVER (PARTITION BY deal_key, vin ORDER BY ft.global_rownum) AS synthetic_rownum
    FROM final_transactions ft
    LEFT JOIN bronze.ns.account a ON ft.account_key = CAST(a.id AS STRING)
  ),
  driver_rownum_4141 AS (
    SELECT
      transaction_key_unique,
      ROW_NUMBER() OVER (
        PARTITION BY deal_key, vin
        ORDER BY global_rownum
      ) AS driver_rownum
    FROM final_transactions_with_account
    WHERE acctnumber = '4141' AND transaction_type = 'REVENUE'
  ),
  final_transactions_with_driver_count AS (
    SELECT
      fta.*,
      CASE 
        WHEN dr.driver_rownum = 1 THEN TRUE
        ELSE FALSE
      END AS is_driver_count
    FROM final_transactions_with_account fta
    LEFT JOIN driver_rownum_4141 dr
      ON fta.transaction_key_unique = dr.transaction_key_unique
  )
SELECT 
  -- Use transaction_key_unique as transaction_key in the final output
  transaction_key_unique AS transaction_key,
  -- ... all other columns ...
  deal_key, account_key, netsuite_posting_date_key, netsuite_posting_time_key, revenue_recognition_date_key, revenue_recognition_time_key, vin, month, year, transaction_type, transaction_category, transaction_subcategory, amount_cents, amount_dollars, allocation_method, allocation_factor, _source_table, CURRENT_TIMESTAMP() AS _load_timestamp, has_credit_memo, credit_memo_date_key, credit_memo_time_key, is_driver_count
FROM final_transactions_with_driver_count
WHERE account_key IS NOT NULL

) AS source
ON target.transaction_key = source.transaction_key

WHEN MATCHED THEN
  UPDATE SET
    target.deal_key = source.deal_key,
    target.account_key = source.account_key,
    target.netsuite_posting_date_key = source.netsuite_posting_date_key,
    target.netsuite_posting_time_key = source.netsuite_posting_time_key,
    target.revenue_recognition_date_key = source.revenue_recognition_date_key,
    target.revenue_recognition_time_key = source.revenue_recognition_time_key,
    target.vin = source.vin,
    target.month = source.month,
    target.year = source.year,
    target.transaction_type = source.transaction_type,
    target.transaction_category = source.transaction_category,
    target.transaction_subcategory = source.transaction_subcategory,
    target.amount_cents = source.amount_cents,
    target.amount_dollars = source.amount_dollars,
    target.allocation_method = source.allocation_method,
    target.allocation_factor = source.allocation_factor,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp,
    target.has_credit_memo = source.has_credit_memo,
    target.credit_memo_date_key = source.credit_memo_date_key,
    target.credit_memo_time_key = source.credit_memo_time_key,
    target.is_driver_count = source.is_driver_count

WHEN NOT MATCHED THEN
  INSERT (
    transaction_key, deal_key, account_key, netsuite_posting_date_key, netsuite_posting_time_key,
    revenue_recognition_date_key, revenue_recognition_time_key, vin, month, year,
    transaction_type, transaction_category, transaction_subcategory, amount_cents, amount_dollars,
    allocation_method, allocation_factor, _source_table, _load_timestamp, has_credit_memo, credit_memo_date_key, credit_memo_time_key, is_driver_count
  )
  VALUES (
    source.transaction_key, source.deal_key, source.account_key, source.netsuite_posting_date_key, source.netsuite_posting_time_key,
    source.revenue_recognition_date_key, source.revenue_recognition_time_key, source.vin, source.month, source.year,
    source.transaction_type, source.transaction_category, source.transaction_subcategory, source.amount_cents, source.amount_dollars,
    source.allocation_method, source.allocation_factor, source._source_table, source._load_timestamp, source.has_credit_memo, source.credit_memo_date_key, source.credit_memo_time_key, source.is_driver_count
  ); 