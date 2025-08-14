-- models/silver/fact/fact_deal_netsuite_transactions.sql
-- Normalized NetSuite fact table with account foreign keys for flexible pivoting
-- BASE TABLE: Contains both posted and unposted transactions

-- Drop and recreate table to ensure correct schema

DROP VIEW IF EXISTS silver.finance.fact_deal_netsuite_transactions;
DROP TABLE IF EXISTS silver.finance.fact_deal_netsuite_transactions_base;

-- 1. Create the normalized fact base table (contains all transactions)
CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_netsuite_transactions_base (
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
  
  -- NEW: Posting status tracking
  netsuite_posting_status STRING, -- 'POSTED', 'PENDING', 'UNPOSTED' for debugging
  
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
MERGE INTO silver.finance.fact_deal_netsuite_transactions_base AS target
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

    UNION

    SELECT
        bd.deal_id,
        UPPER(bd.vin) AS vin
    FROM silver.finance.bridge_vin_deal bd
    WHERE bd.is_active = TRUE
        AND bd.vin IS NOT NULL
        AND LENGTH(bd.vin) = 17
        AND bd.deal_id IS NOT NULL
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
  
  -- Accounts handled by DIRECT method (by account number for maintainability)
  direct_method_accounts AS (
    SELECT account_number FROM VALUES
      ('5100'), -- COR - LEASE BUYOUT - TYPE B FIX: Major GENJRNL variances
      ('5121'), -- COR - GAP CHARGEBACK - TYPE B FIX: Unallocated variances
      ('5138'), -- COR - PENALTIES AND LATE FEES - TYPE B FIX: Operational transactions
      ('5139'), -- COR - REGISTRATION - TYPE B FIX: Non-posted transaction issues
      ('5140'), -- COR - SALES TAX - TYPE B FIX: Non-posted transaction issues  
      ('5141'), -- COR - TITLE ONLY FEES - TYPE B FIX: Non-posted transaction issues
      ('5210'), -- COR - ACQUISITION - TYPE B FIX: MAJOR $400K+/month GENJRNL variances
      ('5213'), -- TRANSPORTATION - TYPE B FIX: Small operational variances
      ('5301'), -- IC FUNDING CLERKS
      ('5304'), -- IC PAYOFF TEAM  
      ('5320'), -- IC TITLE CLERKS
      ('5330'), -- EMPLOYEE BENEFITS
      ('5340'), -- PAYROLL TAX
      ('5400'), -- COR - PAYOFF EXPENSE - ADDED to capture GENJRNL/BILLCRED/CC expenses without deal_id
      ('5401'), -- COR - SALES TAX EXPENSE - ADDED to capture GENJRNL expenses without deal_id
      ('5402'), -- COR - REGISTRATION EXPENSE - ADDED to capture GENJRNL/BILLCRED/CC expenses without deal_id
      ('5403'), -- COR - CUSTOMER EXPERIENCE - ADDED to capture CC/BILLCRED/GENJRNL expenses without deal_id
      ('5404'), -- COR - PENALTIES - ADDED to capture GENJRNL expenses without deal_id
      ('5510'), -- POSTAGE
      ('5520'), -- BANK BUYOUT FEES
      ('5530'), -- TITLE COR
      ('6000'), -- MANAGER COMP - TYPE B FIX: Major payroll variances from GENJRNL+PAY CHK
      ('6010'), -- COMMISSION - TYPE B FIX: Major commission variances from GENJRNL+PAY CHK
      ('6011'), -- SALESPERSON GUARANTEE - ADDED to fix reconciliation issues
      ('6012'), -- FUNDING CLERK COMP - TYPE B FIX: Compensation variances from payroll
      ('6030'), -- HOURLY - TYPE B FIX: Major payroll variances from GENJRNL+PAY CHK
      ('6040'), -- DEVELOPER COMP - TYPE B FIX: Major compensation variances from payroll
      ('6041'), -- IC DEVELOPER COMP - ADDED to fix reconciliation issues
      ('6100'), -- EMP BENEFITS - TYPE B FIX: Benefits variances from GENJRNL+BILL  
      ('6101'), -- EMP ENGAGEMENT - ADDED to fix reconciliation issues
      ('6110'), -- SALARIES
      ('6120'), -- COMMISSIONS
      ('6130'), -- BONUSES
      ('6140'), -- EMPLOYEE BENEFITS
      ('6150'), -- PAYROLL TAX
      ('6200'), -- PAYROLL TAX - TYPE B FIX: Major tax variances from GENJRNL+PAY CHK
      ('6210'), -- CONTRACTOR FEES
      ('6310'), -- RENT
      ('6320'), -- UTILITIES
      ('6330'), -- INSURANCE
      ('6340'), -- OFFICE SUPPLIES
      ('6350'), -- TELEPHONE
      ('6410'), -- PROFESSIONAL FEES
      ('6420'), -- LEGAL FEES
      ('6510'), -- ADVERTISING
      ('6520'), -- MARKETING
      ('6530'), -- DIGITAL MARKETING - ADDED to fix reconciliation issues
      ('6540'), -- SMS MARKETING - ADDED to fix reconciliation issues
      ('6560'), -- PROMOTIONAL MARKETING - ADDED to fix reconciliation issues
      ('6570'), -- AFFILIATE MARKETING - ADDED to fix reconciliation issues
      ('7001'), -- PAYOFF VARIANCE - TYPE B FIX: Operational adjustment transactions
      ('7004'), -- SALES TAX VARIANCE - TYPE B FIX: Tax variance adjustments
      ('7005'), -- REGISTRATION VARIANCE - TYPE B FIX: Registration variance adjustments
      ('7007'), -- PENALTIES - TYPE B FIX: Penalty transactions
      ('7100'), -- INSURANCE - ADDED to fix reconciliation issues
      ('7110'), -- OFFICE SUPPLIES - ADDED to fix reconciliation issues  
      ('7120'), -- POSTAGE - ADDED to fix reconciliation issues
      ('7125'), -- RECRUITING EXPENSE - ADDED to fix reconciliation issues
      ('7130'), -- PROFESSIONAL FEES - ADDED to fix reconciliation issues
      ('7131'), -- BUSINESS LICENSE AND REGISTRATIONS - ADDED to fix reconciliation issues
      ('7140'), -- SUBSCRIPTION FEES - ADDED to fix reconciliation issues
      ('7141'), -- BANK FEES (account 450)
      ('7160'), -- TRAVEL & ENTERTAINMENT - ADDED to capture individual transactions instead of allocation
      ('7170'), -- RENT & LEASE - ADDED to capture individual transactions instead of allocation  
      ('7170A'), -- RENT AMORTIZATION - ADDED to fix reconciliation issues
      ('7171'), -- UTILITIES - ADDED to fix reconciliation issues
      ('7172'), -- BUILDING MAINTENANCE & EXPENSE - ADDED to fix reconciliation issues
      ('7174'), -- VEHICLE MAINTENANCE - ADDED to capture individual transactions instead of allocation
      ('7175'), -- DEPRECIATION EXPENSE - ADDED to fix reconciliation issues
      ('7176'), -- AMORTIZATION EXPENSE - ADDED to fix reconciliation issues
      ('7180'), -- INTERNET & TELEPHONE - ADDED to fix reconciliation issues
      ('7190'), -- SALES EXPENSE - ADDED to fix reconciliation issues
      ('7192'), -- ADDED to fix reconciliation issues
      ('8010'), -- STATE INCOME TAX - ADDED to fix reconciliation issues
      ('8011')  -- FEDERAL INCOME TAX - ADDED to fix reconciliation issues
      -- REMOVED: ('4106') -- REV - RESERVE BONUS - REMOVED: Revenue accounts shouldn't use DIRECT method
      -- REMOVED: ('4190')  -- REBATES (account 563) - should use normal revenue allocation methods
    AS t(account_number)
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
          ('4115', 'REVENUE', 'VSC', 'REINSURANCE'),
          
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
      a.acctnumber as account_number, -- Add account number for easier filtering
      -- Flag for accounts handled by DIRECT method
      CASE WHEN dma.account_number IS NOT NULL THEN TRUE ELSE FALSE END as is_direct_method_account,
      -- Use business override if exists, otherwise auto-classify based on account number ranges AND NetSuite account type
      COALESCE(
        bao.transaction_type,
        CASE 
          -- Income Statement account number classification (matches your income statement structure)
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 THEN 'REVENUE'                    -- 4000 series = Revenue
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 5000 AND 5999 THEN 'COST_OF_REVENUE'           -- 5000 series = Cost of Revenue
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 6000 AND 7999 THEN 'EXPENSE'                   -- 6000-7000 series = Operating Expenses
          WHEN CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 8000 AND 8999 THEN 'EXPENSE'                   -- 8000 series = Tax Expenses (treat as regular expenses)
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
    LEFT JOIN direct_method_accounts dma ON a.acctnumber = dma.account_number
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
    INNER JOIN bronze.ns.account a ON so.account = a.id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Exclude multi-VIN transactions
        AND t.custbody_le_deal_id IS NOT NULL
        AND t.custbody_le_deal_id != 0
        AND t.abbrevtype IN ('SALESORD','CREDITMEMO','CREDMEM','INV','GENJRNL','BILL','BILLCRED','CC','INV WKST')  -- Include invoice & journal revenue entries
        AND am.transaction_type = 'REVENUE'
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)  -- Approved or no approval needed
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND so.amount != 0
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
        -- Remove GL EXISTS requirement for base revenue accounts; keep it for others
        AND (
          a.acctnumber IN ('4105','4110','4120','4130','4141','4110A','4120A')
          OR EXISTS (
            SELECT 1 FROM bronze.ns.transactionaccountingline tal
            WHERE tal.transaction = t.id AND tal.account = so.account AND tal.posting = 'T'
              AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
          )
        )
    GROUP BY UPPER(t.custbody_leaseend_vinno), so.account
  ),

  -- VIN-matching expenses - USE TRANSACTIONLINE for expense accounts
  -- MUST have BOTH VIN AND Deal ID to be considered VIN-matching
  -- Only match EXACT 17-character VINs (no commas or multiple VINs)
  vin_matching_expenses AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        tl.expenseaccount as account,
        SUM(tl.netamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Exclude multi-VIN transactions
        AND t.custbody_le_deal_id IS NOT NULL
        AND t.custbody_le_deal_id != 0
        AND t.abbrevtype IN ('BILL', 'GENJRNL', 'BILLCRED', 'CC', 'CC CRED', 'CHK', 'ITEMSHIP', 'PAY CHK', 'DEP', 'ITEM RCP', 'INV WKST')  -- Include INV WKST for inventory adjustments
        AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)  -- Approved or no approval needed
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND tl.netamount != 0
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
    GROUP BY UPPER(t.custbody_leaseend_vinno), tl.expenseaccount
  ),
  
  -- VIN-only expenses - USE TRANSACTIONLINE for expense accounts 
  -- Captures VINs that either have no deal_id OR have deal_id but don't match our VIN_MATCH criteria
  -- Also exclude accounts 5110, 5120, 5110A, 5120A from VIN-only matching
  -- Only match EXACT 17-character VINs (no commas or multiple VINs)
  -- SIGN CORRECTION: Negate amount to match income statement format (expenses should be negative)
  vin_only_expenses AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        tl.expenseaccount as account,
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        YEAR(t.trandate) as transaction_year,
        MONTH(t.trandate) as transaction_month,
        t.trandate,
        SUM(tl.netamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Exclude multi-VIN transactions
        -- VIN_ONLY: Only single-VIN expense transactions WITHOUT deal_ids
        AND (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)  -- VIN_ONLY: Only transactions without deal_ids
        AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
        AND t.abbrevtype IN ('BILL', 'GENJRNL', 'BILLCRED', 'CC', 'CC CRED', 'CHK', 'ITEMSHIP', 'PAY CHK', 'DEP', 'ITEM RCP', 'INV WKST')  -- Include INV WKST for inventory adjustments
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND tl.netamount != 0
        AND t.trandate IS NOT NULL
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
    GROUP BY UPPER(t.custbody_leaseend_vinno), tl.expenseaccount, DATE_FORMAT(t.trandate, 'yyyy-MM'), YEAR(t.trandate), MONTH(t.trandate), t.trandate
  ),
  
  -- Multi-VIN CTEs removed - we now capture multi-VIN transactions as unallocated for GL reconciliation
  -- (Removed complex multi_vin_expenses and multi_vin_revenue CTEs)
  
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
    INNER JOIN bronze.ns.account a ON so.account = a.id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Exclude multi-VIN transactions
        AND (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)  -- VIN_ONLY: Only transactions without deal_ids
        AND (
            -- For accounts 4110A and 4120A, include all transaction types including GENJRNL
            (a.acctnumber IN ('4110A', '4120A') AND t.abbrevtype IN ('SALESORD','CREDITMEMO','CREDMEM','INV','BILL','BILLCRED','CC','INV WKST','GENJRNL'))
            OR 
            -- For other accounts, exclude GENJRNL to prevent double-counting
            (a.acctnumber NOT IN ('4110A', '4120A') AND t.abbrevtype IN ('SALESORD','CREDITMEMO','CREDMEM','INV','BILL','BILLCRED','CC','INV WKST'))
        )
        AND am.transaction_type = 'REVENUE'
        AND am.transaction_subcategory != 'CHARGEBACK'  -- Exclude chargebacks (handled separately)
        AND t.trandate IS NOT NULL
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND so.amount != 0
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
        AND a.acctnumber NOT IN ('4110B', '4120B')  -- Exclude volume bonus accounts to prevent double-counting with NON_VIN_VOLUME_BONUS
        -- Remove GL EXISTS requirement for base revenue accounts; keep it for others
        AND (
          a.acctnumber IN ('4105','4110','4120','4130','4141','4110A','4120A')
          OR EXISTS (
            SELECT 1 FROM bronze.ns.transactionaccountingline tal
            WHERE tal.transaction = t.id AND tal.account = so.account AND tal.posting = 'T'
              AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
          )
        )
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
          t.id as transaction_id, -- Add transaction ID for exclusion logic
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
      INNER JOIN bronze.ns.account a ON so.account = a.id
      LEFT JOIN deal_vins dv ON t.custbody_le_deal_id = dv.deal_id
      WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
          AND t.custbody_le_deal_id IS NOT NULL
          AND am.transaction_type = 'REVENUE'
          AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
          AND so.amount != 0
          -- Remove GL EXISTS requirement for base revenue accounts; keep it for others
          AND (
            a.acctnumber IN ('4105','4110','4120','4130','4141','4110A','4120A')
            OR EXISTS (
              SELECT 1 FROM bronze.ns.transactionaccountingline tal
              WHERE tal.transaction = t.id AND tal.account = so.account AND tal.posting = 'T'
                AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
            )
          )
    )
    -- Only include transactions that still can't be resolved after VIN lookup
    -- AND exclude transactions already captured by VIN-matching to prevent double-counting
    SELECT
        revenue_recognition_period,
        account,
        SUM(amount) AS total_amount
    FROM missing_vin_with_lookup mvwl
    INNER JOIN account_mappings am ON mvwl.account = am.account_id
    WHERE resolved_vin IS NULL  -- Only transactions that still have no VIN after lookup
      AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
      -- CRITICAL FIX: Exclude transactions already captured by VIN-matching to prevent double-counting
      AND NOT EXISTS (
          SELECT 1 
          FROM bronze.ns.salesinvoiced so2
          INNER JOIN bronze.ns.transaction t2 ON so2.transaction = t2.id
          WHERE t2.id = mvwl.transaction_id
            AND LENGTH(t2.custbody_leaseend_vinno) = 17
            AND t2.custbody_leaseend_vinno IS NOT NULL
            AND t2.custbody_leaseend_vinno NOT LIKE '%,%'
            AND t2.custbody_le_deal_id IS NOT NULL
            AND t2.custbody_le_deal_id != 0
            AND so2.account = mvwl.account
      )
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
      INNER JOIN bronze.ns.account a ON so.account = a.id
      LEFT JOIN deal_vins dv ON t.custbody_le_deal_id = dv.deal_id
      WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
          AND t.custbody_le_deal_id IS NOT NULL
          AND am.transaction_type = 'REVENUE'
          AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
          AND so.amount != 0
          -- Remove GL EXISTS requirement for base revenue accounts; keep it for others
          AND (
            a.acctnumber IN ('4105','4110','4120','4130','4141','4110A','4120A')
            OR EXISTS (
              SELECT 1 FROM bronze.ns.transactionaccountingline tal
              WHERE tal.transaction = t.id AND tal.account = so.account AND tal.posting = 'T'
                AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
            )
          )
    )
    -- Include transactions that were resolved via VIN lookup
    SELECT
        resolved_vin as vin,
        account,
        SUM(amount) AS total_amount
    FROM missing_vin_with_lookup mvwl2
    INNER JOIN account_mappings am ON mvwl2.account = am.account_id
    WHERE resolved_vin IS NOT NULL  -- Only transactions resolved via VIN lookup
      AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
    GROUP BY resolved_vin, account
  ),
  

  
  -- Unallocated transactions (not tied to any deal) - SEPARATE from deal-based transactions
  -- ENHANCED: Handle both salesinvoiced and transactionline for revenue accounts
  unallocated_revenue AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        so.account,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    INNER JOIN bronze.ns.account a ON so.account = a.id  -- Add account table for exclusion logic
    WHERE (((t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)
             AND (t.custbody_leaseend_vinno IS NULL 
                  OR LENGTH(t.custbody_leaseend_vinno) != 17  -- Include invalid VIN lengths
                  OR t.custbody_leaseend_vinno LIKE '% %'))   -- Include VINs with spaces (not real VINs)
           OR (t.custbody_le_deal_id IS NOT NULL AND t.custbody_le_deal_id != 0
               AND (t.custbody_leaseend_vinno IS NULL 
                    OR LENGTH(t.custbody_leaseend_vinno) != 17  -- Include invalid VIN lengths
                    OR t.custbody_leaseend_vinno LIKE '% %')))  -- Include VINs with spaces (not real VINs)
        AND (t.custbody_leaseend_vinno IS NULL OR t.custbody_leaseend_vinno NOT LIKE '%,%')  -- EXCLUDE multi-VIN with NULL handling
        AND t.trandate IS NOT NULL
        AND t.abbrevtype IN ('SALESORD','CREDITMEMO','CREDMEM','INV','GENJRNL','BILL','BILLCRED','CC','INV WKST')  -- Include GENJRNL for revenue like 4106 Reserve Bonus
        AND am.transaction_type = 'REVENUE'
        AND am.transaction_subcategory != 'CHARGEBACK'  -- Exclude chargebacks (handled separately)
        -- CRITICAL FIX: Only exclude volume bonus accounts to prevent double-counting with VIN_ONLY
        -- Advance and Cost accounts legitimately have both corporate and deal-specific amounts
        AND a.acctnumber NOT IN ('4110B', '4120B')
        -- SURGICAL FIX: Exclude specific cancelled/corrected transactions like 1376483
        AND NOT (
            t.abbrevtype = 'GENJRNL' 
            AND t.memo LIKE '%Month end Adj%'
            AND NOT EXISTS (
                SELECT 1 FROM bronze.ns.transactionaccountingline tal
                WHERE tal.transaction = t.id AND tal.account = so.account
                  AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
            )
        )
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND so.amount != 0
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), so.account
  ),
  
  -- This is the single, correct CTE for all unallocated expenses
  unallocated_expenses_consolidated AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        tl.expenseaccount as account,
        SUM(tl.netamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)
        AND (t.custbody_leaseend_vinno IS NULL 
             OR LENGTH(t.custbody_leaseend_vinno) != 17  -- Include invalid VIN lengths
             OR t.custbody_leaseend_vinno LIKE '% %')    -- Include VINs with spaces (not real VINs)
        AND (t.custbody_leaseend_vinno IS NULL OR t.custbody_leaseend_vinno NOT LIKE '%,%')  -- EXCLUDE multi-VIN with NULL handling
        AND t.trandate IS NOT NULL
        AND am.transaction_type IN ('COST_OF_REVENUE', 'EXPENSE', 'OTHER_EXPENSE')
        AND t.abbrevtype IN ('BILL', 'GENJRNL', 'BILLCRED', 'CC', 'CC CRED', 'CHK', 'ITEMSHIP', 'PAY CHK', 'DEP', 'ITEM RCP', 'INV WKST')  -- Include INV WKST for inventory adjustments
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)  -- Standard approval filter for consistency
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND tl.netamount != 0
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), tl.expenseaccount
    
    -- Note: Removed complex partial multi-VIN allocation logic
    -- Multi-VIN transactions are now captured as unallocated for GL reconciliation accuracy
  ),
  
  -- Volume bonus transactions - capture posted GL lines to match GL totals and counts
  non_vin_volume_bonus_gl AS (
    SELECT
      tal.account,
      t.id AS transaction_id,
      t.trandate,
      YEAR(t.trandate) AS transaction_year,
      MONTH(t.trandate) AS transaction_month,
      tal.amount AS gl_amount,
      a.acctnumber,
      tal.transactionline AS gl_line_id
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN bronze.ns.account a ON tal.account = a.id
    WHERE a.acctnumber IN ('4110B', '4120B')
      AND tal.posting = 'T'
      AND tal.amount != 0
      AND t.trandate IS NOT NULL
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
  ),

  -- GL-only posted expenses for specific accounts missing from transactionline (one-off NS posted GL entries)
  gl_only_expenses AS (
    SELECT
      tal.account as account,
      DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
      SUM(tal.amount) as total_amount
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN bronze.ns.account aexp ON tal.account = aexp.id
    WHERE tal.posting = 'T'
      AND am.transaction_type IN ('COST_OF_REVENUE','EXPENSE','OTHER_EXPENSE')
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.transactionline tl2
        WHERE tl2.transaction = t.id AND tl2.expenseaccount = tal.account
      )
    GROUP BY tal.account, DATE_FORMAT(t.trandate, 'yyyy-MM')
  ),
  
  -- Combine VIN-matching and allocated amounts
  final_transactions_base AS (
    -- VIN-matching revenue transactions (consolidated by NetSuite transaction ID)
    SELECT
      CONCAT(t.custbody_le_deal_id, '_', so.account, '_', t.id, '_REVENUE') as transaction_key,
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
      -- Use original signs as they appear in NetSuite
      CAST(ROUND(SUM(so.amount) * 100) AS BIGINT) as amount_cents,
      CAST(SUM(so.amount) AS DECIMAL(15,2)) as amount_dollars,
      'VIN_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      CASE WHEN MAX(CASE WHEN cmv.vin IS NOT NULL THEN 1 ELSE 0 END) = 1 THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(MAX(cmv.credit_memo_date), 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(MAX(cmv.credit_memo_date), 'HHmmss') AS INT), 0) AS credit_memo_time_key,
      -- Posting status fields
      CASE WHEN MAX(t.posting) = 'T' THEN 'POSTED' ELSE 'UNPOSTED' END AS netsuite_posting_status
    FROM bronze.ns.salesinvoiced so
    INNER JOIN bronze.ns.transaction t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    INNER JOIN bronze.ns.account a ON so.account = a.id
    LEFT JOIN credit_memo_vins cmv ON UPPER(t.custbody_leaseend_vinno) = cmv.vin
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
      AND t.custbody_leaseend_vinno IS NOT NULL
      AND t.custbody_leaseend_vinno NOT LIKE '%,%'
      AND t.custbody_le_deal_id IS NOT NULL
      AND t.custbody_le_deal_id != 0
      AND t.abbrevtype IN ('SALESORD','CREDITMEMO','CREDMEM','INV','GENJRNL','BILL','BILLCRED','CC','INV WKST')  -- Include invoice & journal revenue entries
      AND am.transaction_type = 'REVENUE'
      AND am.transaction_subcategory != 'CHARGEBACK'  -- Exclude chargebacks (handled separately)
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
      AND so.amount != 0
      AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
    GROUP BY t.id, t.custbody_le_deal_id, so.account, t.trandate, t.custbody_leaseend_vinno, am.transaction_type, am.transaction_category, am.transaction_subcategory, a.acctnumber

        UNION ALL

    -- VIN-matching expense transactions (consolidated by NetSuite transaction ID)
    SELECT
      CONCAT(t.custbody_le_deal_id, '_', tl.expenseaccount, '_', t.id, '_EXPENSE') as transaction_key,
      CAST(t.custbody_le_deal_id AS STRING) as deal_key,
      CAST(tl.expenseaccount AS STRING) as account_key,
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
      CAST(ROUND(SUM(tl.netamount) * 100) AS BIGINT) as amount_cents,
      CAST(SUM(tl.netamount) AS DECIMAL(15,2)) as amount_dollars,
      'VIN_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      CASE WHEN MAX(CASE WHEN cmv.vin IS NOT NULL THEN 1 ELSE 0 END) = 1 THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(MAX(cmv.credit_memo_date), 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(MAX(cmv.credit_memo_date), 'HHmmss') AS INT), 0) AS credit_memo_time_key,
      -- Posting status fields
      CASE WHEN MAX(t.posting) = 'T' THEN 'POSTED' ELSE 'UNPOSTED' END AS netsuite_posting_status
    FROM bronze.ns.transactionline tl
    INNER JOIN bronze.ns.transaction t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    LEFT JOIN credit_memo_vins cmv ON UPPER(t.custbody_leaseend_vinno) = cmv.vin
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
      AND t.custbody_leaseend_vinno IS NOT NULL
      AND t.custbody_leaseend_vinno NOT LIKE '%,%'
      AND t.custbody_le_deal_id IS NOT NULL
      AND t.custbody_le_deal_id != 0
      AND t.abbrevtype IN ('BILL', 'GENJRNL', 'BILLCRED', 'CC', 'CC CRED', 'CHK', 'ITEMSHIP', 'PAY CHK', 'DEP', 'ITEM RCP', 'INV WKST')  -- Include INV WKST for inventory adjustments
      AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
      AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
      AND tl.netamount != 0
      AND am.is_direct_method_account = FALSE
    GROUP BY t.id, t.custbody_le_deal_id, tl.expenseaccount, t.trandate, t.custbody_leaseend_vinno, am.transaction_type, am.transaction_category, am.transaction_subcategory

    UNION ALL

    -- Chargeback revenue transactions (from transactionline to match Income Statement source)
    -- NetSuite Income Statement uses transactionline data for chargeback accounts, not salesinvoiced
    -- Chargebacks are negative values in the income statement (they reduce revenue)
    SELECT
      CONCAT('CHARGEBACK_', tl.expenseaccount, '_', t.id, '_REVENUE') as transaction_key,
      NULL as deal_key,  -- Chargebacks typically don't have deal IDs
      CAST(tl.expenseaccount AS STRING) as account_key,
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
      CAST(ROUND(-tl.netamount * 100) AS BIGINT) as amount_cents, -- Negate chargeback amounts to match diff report
      CAST(-tl.netamount AS DECIMAL(15,2)) as amount_dollars, -- Negate chargeback amounts to match diff report
      'CHARGEBACK' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields
      CASE WHEN t.posting = 'T' THEN 'POSTED' ELSE 'UNPOSTED' END AS netsuite_posting_status
    FROM bronze.ns.transactionline tl
    INNER JOIN bronze.ns.transaction t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE am.transaction_subcategory = 'CHARGEBACK'  -- Only chargeback accounts (4107, 4111, 4121)
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)  -- Approved or no approval needed
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND tl.netamount != 0

    UNION ALL

    -- Chargeback revenue transactions from posted GL (covers revenue-side chargebacks not in transactionline.expenseaccount)
    SELECT
      CONCAT('GL_CHARGEBACK_', tal.account, '_', t.id, '_', tal.transactionline, '_REVENUE') as transaction_key,
      NULL as deal_key,
      CAST(tal.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      UPPER(COALESCE(t.custbody_leaseend_vinno, '')) as vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am_rev.transaction_type,
      am_rev.transaction_category,
      am_rev.transaction_subcategory,
      -- Chargebacks should reduce revenue; enforce negative sign
      CAST(ROUND(-ABS(tal.amount) * 100) AS BIGINT) as amount_cents,
      CAST(-ABS(tal.amount) AS DECIMAL(15,2)) as amount_dollars,
      'GL_CHARGEBACK' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(0 AS INT) AS credit_memo_date_key,
      CAST(0 AS INT) AS credit_memo_time_key,
      'POSTED' AS netsuite_posting_status
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN account_mappings am_rev ON tal.account = am_rev.account_id
    WHERE am_rev.transaction_subcategory = 'CHARGEBACK'
      AND tal.posting = 'T'
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND tal.amount != 0
      -- Avoid double-counting with expense-side chargeback capture
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.transactionline tlx 
        WHERE tlx.transaction = t.id AND tlx.expenseaccount = tal.account
      )

    UNION ALL

    -- DIRECT method transactions for specific problematic accounts
    -- Properly deduplicated by business keys to prevent same transaction appearing multiple times
    SELECT
      CONCAT('DIRECT_', account_id, '_', deal_id, '_', transaction_id, '_', DATE_FORMAT(trandate, 'yyyyMMdd'), '_', CAST(amount_dollars * 100 AS BIGINT)) as transaction_key,
      CAST(deal_id AS STRING) as deal_key,
      CAST(account_id AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      UPPER(COALESCE(vin, '')) as vin,
      MONTH(trandate) as month,
      YEAR(trandate) as year,
      transaction_type,
      transaction_category,
      transaction_subcategory,
      -- Use the already-calculated amount_dollars (which has sign flipping logic)
      CAST(ROUND(amount_dollars * 100) AS BIGINT) as amount_cents,
      amount_dollars,
      'DIRECT' as allocation_method,  -- Direct capture for problematic accounts
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      source_table as _source_table,
      FALSE AS has_credit_memo,  -- Credit memos handled separately
      CAST(0 AS INT) AS credit_memo_date_key,
      CAST(0 AS INT) AS credit_memo_time_key,
      -- Posting status fields: treat DIRECT as posted to avoid excluding valid GL-backed entries in view
      'POSTED' AS netsuite_posting_status
    FROM (
      -- Deduplicate by business logic first - MODIFIED for 100% accuracy
      SELECT
        t.id as transaction_id,
        COALESCE(t.custbody_le_deal_id, 0) as deal_id,
        COALESCE(tl.expenseaccount, so.account) as account_id,
        t.trandate,
        t.custbody_leaseend_vinno as vin,
        am.transaction_type,
        am.transaction_category,
        am.transaction_subcategory,
        -- Use original signs as they appear in NetSuite
        COALESCE(tl.netamount, so.amount) as amount_dollars,
        CASE WHEN tl.id IS NOT NULL THEN 'bronze.ns.transactionline' ELSE 'bronze.ns.salesinvoiced' END as source_table,
               ROW_NUMBER() OVER (
           PARTITION BY 
             t.id,  -- Use transaction ID as primary deduplication key for 100% accuracy
             COALESCE(tl.id, so.uniquekey)  -- Include line ID to preserve separate line items
           ORDER BY t.id, COALESCE(tl.id, so.uniquekey)
         ) as rn
      FROM bronze.ns.transaction t
      LEFT JOIN bronze.ns.transactionline tl ON t.id = tl.transaction 
      LEFT JOIN bronze.ns.salesinvoiced so ON t.id = so.transaction 
      INNER JOIN account_mappings am ON COALESCE(tl.expenseaccount, so.account) = am.account_id
      WHERE (tl.id IS NOT NULL OR so.uniquekey IS NOT NULL)  -- Must have either expense or revenue line
          AND am.is_direct_method_account = TRUE  -- Only process direct method accounts
          AND t.abbrevtype IN ('BILL', 'GENJRNL', 'BILLCRED', 'CC', 'CC CRED', 'INV', 'CHK', 'CREDMEM', 'CREDITMEMO', 'ITEMSHIP', 'PAY CHK', 'DEP', 'ITEM RCP', 'INV WKST', 'SALESORD')  -- Include ALL transaction types for DIRECT method 100% accuracy
          AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
          AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
          AND (t.posting = 'T' OR t.posting IS NULL)  -- Base keeps both; gold view filters to posted
          AND COALESCE(tl.netamount, so.amount) != 0
          -- REMOVED: Future date filter to include legitimate future-dated posted transactions
          -- AND t.trandate <= CURRENT_DATE()  -- Only include transactions up to today
    ) deduped
    WHERE rn = 1  -- Only take one record per business transaction

    UNION ALL

    -- Additional DIRECT method capture for inventory transactions (ITEMSHIP, ITEM RCP) 
    -- These transactions bypass transactionline and go directly to GL via transactionaccountingline
    SELECT
      CONCAT('DIRECT_GL_', tal.account, '_', COALESCE(t.custbody_le_deal_id, 0), '_', t.id, '_', DATE_FORMAT(t.trandate, 'yyyyMMdd'), '_', CAST(ROUND(tal.amount * 100) AS BIGINT)) as transaction_key,
      CAST(COALESCE(t.custbody_le_deal_id, 0) AS STRING) as deal_key,
      CAST(tal.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      UPPER(COALESCE(t.custbody_leaseend_vinno, '')) as vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      -- Use original signs as they appear in NetSuite
      CAST(ROUND((CASE 
                    WHEN CAST(REGEXP_EXTRACT(acc_gl.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
                      THEN -tal.amount 
                    ELSE tal.amount 
                  END) * 100) AS BIGINT) as amount_cents,
      CAST(CASE 
            WHEN CAST(REGEXP_EXTRACT(acc_gl.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
              THEN -tal.amount 
            ELSE tal.amount 
          END AS DECIMAL(15,2)) as amount_dollars,
      'DIRECT' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(0 AS INT) AS credit_memo_date_key,
      CAST(0 AS INT) AS credit_memo_time_key,
      -- Posting status fields
      'POSTED' AS netsuite_posting_status  -- GL transactions are always posted
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN bronze.ns.account acc_gl ON tal.account = acc_gl.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    WHERE am.is_direct_method_account = TRUE  -- Only DIRECT method accounts
        AND t.abbrevtype IN ('ITEMSHIP', 'ITEM RCP', 'BILL', 'BILLCRED', 'INV', 'CREDMEM', 'GENJRNL', 'PAY CHK', 'CC', 'CC CRED', 'CHK', 'DEP')  -- Include payroll, journal, cards
        AND tal.posting = 'T'  -- Only posted GL entries
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
        AND tal.amount != 0
        -- Avoid double-counting: exclude transactions already captured in transactionline/salesinvoiced
        AND NOT EXISTS (
            SELECT 1 FROM bronze.ns.transactionline tl2 
            WHERE tl2.transaction = t.id AND tl2.expenseaccount = tal.account
        )

    UNION ALL

    -- GL-posted transactions for NON-DIRECT accounts to capture GENJRNL/PAY CHK and other GL-only postings
    -- Restrict to expense-side categories to avoid double counting revenue GL lines (handled in GL_REV below)
    SELECT
      CONCAT('GL_', tal.account, '_', t.id, '_', tal.transactionline, '_', DATE_FORMAT(t.trandate, 'yyyyMMdd'), '_', CAST(ROUND(tal.amount * 100) AS BIGINT)) as transaction_key,
      CAST(COALESCE(t.custbody_le_deal_id, 0) AS STRING) as deal_key,
      CAST(tal.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      UPPER(COALESCE(t.custbody_leaseend_vinno, '')) as vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND((CASE 
                    WHEN CAST(REGEXP_EXTRACT(acc_gl_v.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
                      THEN -tal.amount 
                    ELSE tal.amount 
                  END) * 100) AS BIGINT) as amount_cents,
      CAST(CASE 
            WHEN CAST(REGEXP_EXTRACT(acc_gl_v.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
              THEN -tal.amount 
            ELSE tal.amount 
          END AS DECIMAL(15,2)) as amount_dollars,
      'GL' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(0 AS INT) AS credit_memo_date_key,
      CAST(0 AS INT) AS credit_memo_time_key,
      'POSTED' AS netsuite_posting_status
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    INNER JOIN bronze.ns.account acc_gl_v ON tal.account = acc_gl_v.id
    WHERE am.is_direct_method_account = FALSE
      AND am.transaction_type IN ('COST_OF_REVENUE','EXPENSE','OTHER_EXPENSE')
      AND tal.posting = 'T'
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND tal.amount != 0
      -- Avoid double-counting: exclude transactions already captured in transactionline/salesinvoiced for the same account
      AND NOT EXISTS (
          SELECT 1 FROM bronze.ns.transactionline tl2 
          WHERE tl2.transaction = t.id AND tl2.expenseaccount = tal.account
            AND SIGN(tl2.netamount) = SIGN(tal.amount)
      )
      AND NOT EXISTS (
          SELECT 1 FROM bronze.ns.salesinvoiced so2 
          WHERE so2.transaction = t.id AND so2.account = tal.account
      )

    UNION ALL

    -- GL-posted transactions specifically for 4110C/4120C cost accounts (ensure cost is fully captured)
    -- These accounts are in the 4xxx family but represent cost; include regardless of mapped transaction_type
    SELECT
      CONCAT('GL_COST_4XXX_', tal.account, '_', t.id, '_', tal.transactionline, '_', DATE_FORMAT(t.trandate, 'yyyyMMdd'), '_', CAST(ROUND(tal.amount * 100) AS BIGINT)) as transaction_key,
      CAST(COALESCE(t.custbody_le_deal_id, 0) AS STRING) as deal_key,
      CAST(tal.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      UPPER(COALESCE(t.custbody_leaseend_vinno, '')) as vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      -- Preserve mapped fields if available
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND((CASE 
                    WHEN CAST(REGEXP_EXTRACT(acc_gl_v.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
                      THEN -tal.amount 
                    ELSE tal.amount 
                  END) * 100) AS BIGINT) as amount_cents,
      CAST(CASE 
            WHEN CAST(REGEXP_EXTRACT(acc_gl_v.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
              THEN -tal.amount 
            ELSE tal.amount 
          END AS DECIMAL(15,2)) as amount_dollars,
      'GL' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(0 AS INT) AS credit_memo_date_key,
      CAST(0 AS INT) AS credit_memo_time_key,
      'POSTED' AS netsuite_posting_status
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    INNER JOIN bronze.ns.account acc_gl_v ON tal.account = acc_gl_v.id
    WHERE tal.posting = 'T'
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND tal.amount != 0
      AND acc_gl_v.acctnumber IN ('4110C','4120C')
      -- Avoid double-counting: exclude transactions already captured in transactionline/salesinvoiced for the same account
      AND NOT EXISTS (
          SELECT 1 FROM bronze.ns.transactionline tl2 
          WHERE tl2.transaction = t.id AND tl2.expenseaccount = tal.account
      )
      AND NOT EXISTS (
          SELECT 1 FROM bronze.ns.salesinvoiced so2 
          WHERE so2.transaction = t.id AND so2.account = tal.account
      )

    UNION ALL

    -- GL-posted revenue for NON-DIRECT accounts to capture GL-only revenue postings (e.g., 4116)
    -- Includes only posted GL entries and avoids double counting when salesinvoiced/transactionline contains the same account line
    SELECT
      CONCAT('GL_REV_', tal.account, '_', t.id, '_', tal.transactionline, '_', DATE_FORMAT(t.trandate, 'yyyyMMdd'), '_', CAST(ROUND(tal.amount * 100) AS BIGINT)) as transaction_key,
      CAST(COALESCE(t.custbody_le_deal_id, 0) AS STRING) as deal_key,
      CAST(tal.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      UPPER(COALESCE(t.custbody_leaseend_vinno, '')) as vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND((CASE 
                    WHEN CAST(REGEXP_EXTRACT(acc_gl_r.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
                      THEN -tal.amount  
                    ELSE tal.amount 
                  END) * 100) AS BIGINT) as amount_cents,
      CAST(CASE 
            WHEN CAST(REGEXP_EXTRACT(acc_gl_r.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
              THEN -tal.amount 
            ELSE tal.amount 
          END AS DECIMAL(15,2)) as amount_dollars,
      'GL' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(0 AS INT) AS credit_memo_date_key,
      CAST(0 AS INT) AS credit_memo_time_key,
      'POSTED' AS netsuite_posting_status
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    INNER JOIN bronze.ns.account acc_gl_r ON tal.account = acc_gl_r.id
    WHERE am.is_direct_method_account = FALSE
      AND am.transaction_type = 'REVENUE'
      AND tal.posting = 'T'
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND tal.amount != 0
      -- Avoid double-count: skip if salesinvoiced already has this transaction/account
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.salesinvoiced so2 
        WHERE so2.transaction = t.id AND so2.account = tal.account
      )
      -- Avoid double-count: skip if transactionline already has this transaction/account (for chargebacks, etc.)
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.transactionline tl2 
        WHERE tl2.transaction = t.id AND tl2.expenseaccount = tal.account
      )

    UNION ALL

    -- GL-only VIN revenue (posted GL lines on 4xxx with valid VIN and deal_id) not present in salesinvoiced
    SELECT
      CONCAT('GL_ONLY_VIN_', tal.account, '_', t.id, '_', tal.transactionline, '_', DATE_FORMAT(t.trandate, 'yyyyMMdd'), '_', CAST(ROUND(tal.amount * 100) AS BIGINT)) as transaction_key,
      CAST(t.custbody_le_deal_id AS STRING) as deal_key,
      CAST(tal.account AS STRING) as account_key,
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
      CAST(ROUND((CASE 
                    WHEN CAST(REGEXP_EXTRACT(acc_gl_v.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
                      THEN -tal.amount 
                    ELSE tal.amount 
                  END) * 100) AS BIGINT) as amount_cents,
      CAST(CASE 
            WHEN CAST(REGEXP_EXTRACT(acc_gl_v.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
              THEN -tal.amount 
            ELSE tal.amount 
          END AS DECIMAL(15,2)) as amount_dollars,
      'GL_ONLY_VIN' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(0 AS INT) AS credit_memo_date_key,
      CAST(0 AS INT) AS credit_memo_time_key,
      'POSTED' AS netsuite_posting_status
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN bronze.ns.account acc_gl_v ON tal.account = acc_gl_v.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    WHERE tal.posting = 'T'
      AND am.transaction_type = 'REVENUE'
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND tal.amount != 0
      AND LENGTH(t.custbody_leaseend_vinno) = 17
      AND t.custbody_leaseend_vinno NOT LIKE '%,%'
      AND t.custbody_le_deal_id IS NOT NULL AND t.custbody_le_deal_id != 0
      -- Dynamically include all revenue subaccounts via account_mappings (no hard-coded list)
      AND am.transaction_type = 'REVENUE'
      -- Avoid double-count: skip if salesinvoiced already has this transaction/account
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.salesinvoiced so2 
        WHERE so2.transaction = t.id AND so2.account = tal.account
      )
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.transactionline tl2 
        WHERE tl2.transaction = t.id AND tl2.expenseaccount = tal.account
      )

    UNION ALL

    -- GL-only NON-VIN revenue (posted GL lines on 4xxx without valid VIN or without deal)
    SELECT
      CONCAT('GL_ONLY_NONVIN_', tal.account, '_', t.id, '_', tal.transactionline, '_', DATE_FORMAT(t.trandate, 'yyyyMMdd'), '_', CAST(ROUND(tal.amount * 100) AS BIGINT)) as transaction_key,
      NULL as deal_key,
      CAST(tal.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      NULL as vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND((CASE 
                    WHEN CAST(REGEXP_EXTRACT(acc_gl_n.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
                      THEN -tal.amount 
                    ELSE tal.amount 
                  END) * 100) AS BIGINT) as amount_cents,
      CAST(CASE 
            WHEN CAST(REGEXP_EXTRACT(acc_gl_n.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
              THEN -tal.amount 
            ELSE tal.amount 
          END AS DECIMAL(15,2)) as amount_dollars,
      'GL_ONLY' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(0 AS INT) AS credit_memo_date_key,
      CAST(0 AS INT) AS credit_memo_time_key,
      'POSTED' AS netsuite_posting_status
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN bronze.ns.account acc_gl_n ON tal.account = acc_gl_n.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    WHERE tal.posting = 'T'
      AND am.transaction_type = 'REVENUE'
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND tal.amount != 0
      AND (t.custbody_leaseend_vinno IS NULL OR LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno LIKE '%,%'
           OR t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)
      -- Dynamically include all revenue subaccounts via account_mappings (no hard-coded list)
      AND am.transaction_type = 'REVENUE'
      -- Avoid double-count: skip if salesinvoiced already has this transaction/account
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.salesinvoiced so2 
        WHERE so2.transaction = t.id AND so2.account = tal.account
      )
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.transactionline tl2 
        WHERE tl2.transaction = t.id AND tl2.expenseaccount = tal.account
      )
    UNION ALL

    -- VIN-only revenue transactions (VIN exists but NOT already captured by VIN_MATCH)
    -- Fix: Only exclude the amount already captured, not the entire VIN+account combination
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
      -- Use original signs as they appear in NetSuite
      CAST(ROUND(vor.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(vor.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'VIN_ONLY_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields
      'POSTED' AS netsuite_posting_status  -- VIN_ONLY transactions are filtered to posted only
    FROM vin_only_revenue vor
    INNER JOIN (
        SELECT vin, MAX(deal_id) as deal_id FROM deal_vins GROUP BY vin
    ) canonical_dv ON vor.vin = canonical_dv.vin
    INNER JOIN account_mappings am ON vor.account = am.account_id
    INNER JOIN bronze.ns.account a ON vor.account = a.id
    WHERE a.acctnumber NOT IN ('4110B', '4120B')  -- Exclude volume bonus accounts from VIN_ONLY_MATCH to prevent double-counting
    GROUP BY canonical_dv.deal_id, vor.account, vor.transaction_period, vor.vin, vor.transaction_month, vor.transaction_year, am.transaction_type, am.transaction_category, am.transaction_subcategory, a.acctnumber, vor.total_amount
    -- No deduplication needed: vin_only_revenue already filters for transactions without deal_ids

    UNION ALL

    -- VIN-only expense transactions (VIN exists but NOT already captured by VIN_MATCH)
    SELECT
      CONCAT(canonical_dv.deal_id, '_', voe.account, '_', DATE_FORMAT(voe.trandate, 'yyyyMMdd'), '_', CAST(ROUND(voe.total_amount * 100) AS BIGINT), '_VIN_ONLY_EXPENSE') as transaction_key,
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
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0) AS credit_memo_time_key,
      -- Posting status fields
      'POSTED' AS netsuite_posting_status  -- VIN_ONLY transactions are filtered to posted only
    FROM vin_only_expenses voe
    INNER JOIN (
        SELECT vin, MAX(deal_id) as deal_id FROM deal_vins GROUP BY vin
    ) canonical_dv ON voe.vin = canonical_dv.vin
    INNER JOIN account_mappings am ON voe.account = am.account_id
    LEFT JOIN credit_memo_vins cmv ON voe.vin = cmv.vin

    UNION ALL

    -- Multi-VIN revenue transactions as unallocated (for GL reconciliation accuracy)
    -- Consolidate by transaction ID to match NetSuite's transaction-level reporting
    SELECT
      CONCAT('MULTI_VIN_', t.id, '_', so.account, '_REVENUE') as transaction_key,
      NULL as deal_key,  -- Multi-VIN transactions can't be accurately allocated to specific deals
      CAST(so.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      NULL as vin,  -- Multi-VIN can't be allocated to single VIN
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      -- Use original signs as they appear in NetSuite
      CAST(ROUND(SUM(so.amount) * 100) AS BIGINT) as amount_cents,
      CAST(SUM(so.amount) AS DECIMAL(15,2)) as amount_dollars,
      'MULTI_VIN_UNALLOCATED' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,  -- Multi-VIN transactions rarely have credit memos
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields
      CASE WHEN MAX(t.posting) = 'T' THEN 'POSTED' ELSE 'UNPOSTED' END AS netsuite_posting_status
    FROM bronze.ns.salesinvoiced so
    INNER JOIN bronze.ns.transaction t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    INNER JOIN bronze.ns.account a ON so.account = a.id
    WHERE t.custbody_leaseend_vinno LIKE '%,%'  -- Multi-VIN transactions only
        AND am.transaction_type = 'REVENUE'
        AND am.transaction_subcategory != 'CHARGEBACK'  -- Exclude chargebacks (handled separately)
        AND t.abbrevtype IN ('SALESORD','CREDITMEMO','CREDMEM','INV','GENJRNL','BILL','BILLCRED','CC','INV WKST')
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND so.amount != 0
        -- Ensure a posted GL line exists for the same transaction/account to match GL
        AND EXISTS (
          SELECT 1 FROM bronze.ns.transactionaccountingline tal
          WHERE tal.transaction = t.id AND tal.account = so.account AND tal.posting = 'T'
            AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
        )
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
    GROUP BY t.id, so.account, t.trandate, am.transaction_type, am.transaction_category, am.transaction_subcategory, a.acctnumber

    UNION ALL

    -- Multi-VIN expense transactions as unallocated (for GL reconciliation accuracy)
    -- Consolidate by transaction ID to match NetSuite's transaction-level reporting
    SELECT
      CONCAT('MULTI_VIN_', t.id, '_', tl.expenseaccount, '_EXPENSE') as transaction_key,
      NULL as deal_key,  -- Multi-VIN transactions can't be accurately allocated to specific deals
      CAST(tl.expenseaccount AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      NULL as vin,  -- Multi-VIN can't be allocated to single VIN
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(SUM(tl.netamount) * 100) AS BIGINT) as amount_cents,
      CAST(SUM(tl.netamount) AS DECIMAL(15,2)) as amount_dollars,
      'MULTI_VIN_UNALLOCATED' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      FALSE AS has_credit_memo,  -- Multi-VIN transactions rarely have credit memos
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields
      CASE WHEN MAX(t.posting) = 'T' THEN 'POSTED' ELSE 'UNPOSTED' END AS netsuite_posting_status
    FROM bronze.ns.transactionline tl
    INNER JOIN bronze.ns.transaction t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE t.custbody_leaseend_vinno LIKE '%,%'  -- Multi-VIN transactions only
        AND t.trandate IS NOT NULL
        AND am.transaction_type IN ('COST_OF_REVENUE', 'EXPENSE', 'OTHER_EXPENSE')
        AND t.abbrevtype IN ('BILL', 'GENJRNL', 'BILLCRED', 'CC', 'CC CRED', 'CHK', 'ITEMSHIP', 'PAY CHK', 'DEP', 'ITEM RCP', 'INV WKST')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND tl.netamount != 0
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method
    GROUP BY t.id, tl.expenseaccount, t.trandate, am.transaction_type, am.transaction_category, am.transaction_subcategory

    UNION ALL

    -- Multi-VIN orphaned logic removed - now handled by MULTI_VIN_UNALLOCATED

    -- VIN-resolved missing revenue transactions (previously would have been allocated)
    SELECT
      CONCAT(canonical_dv.deal_id, '_', vrm.account, '_VIN_RESOLVED_REVENUE') as transaction_key,
      CAST(canonical_dv.deal_id AS STRING) as deal_key,
      CAST(vrm.account AS STRING) as account_key,
      COALESCE(CAST(REPLACE(rrwv.revenue_recognition_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
      CAST(0 AS BIGINT) AS netsuite_posting_time_key,
      COALESCE(CAST(REPLACE(rrwv.revenue_recognition_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
      CAST(0 AS INT) AS revenue_recognition_time_key,
      vrm.vin,
      rrwv.revenue_recognition_month as month,
      rrwv.revenue_recognition_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      -- Use original signs as they appear in NetSuite
      CAST(ROUND(vrm.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(vrm.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'VIN_RESOLVED' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      CASE WHEN cmv.vin IS NOT NULL THEN TRUE ELSE FALSE END AS has_credit_memo,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'yyyyMMdd') AS INT), 0) AS credit_memo_date_key,
      COALESCE(CAST(DATE_FORMAT(cmv.credit_memo_date, 'HHmmss') AS INT), 0) AS credit_memo_time_key,
      -- Posting status fields: VIN_RESOLVED is derived, not a posted GL line
      'UNPOSTED' AS netsuite_posting_status
    FROM vin_resolved_missing_revenue vrm
    INNER JOIN (
        SELECT vin, MAX(deal_id) as deal_id FROM deal_vins GROUP BY vin
    ) canonical_dv ON vrm.vin = canonical_dv.vin
    INNER JOIN revenue_recognition_with_vins rrwv ON canonical_dv.deal_id = rrwv.deal_id
    INNER JOIN account_mappings am ON vrm.account = am.account_id
    LEFT JOIN credit_memo_vins cmv ON vrm.vin = cmv.vin

    UNION ALL
    -- Allocated revenue transactions (missing VIN)
    SELECT
      CONCAT(rrwv.deal_id, '_', mvr.account, '_', rrwv.revenue_recognition_period, '_REVENUE_ALLOC') as transaction_key,
      CAST(rrwv.deal_id AS STRING) as deal_key,
      CAST(mvr.account AS STRING) as account_key,
      COALESCE(CAST(REPLACE(rrwv.revenue_recognition_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
      CAST(0 AS BIGINT) AS netsuite_posting_time_key,
      COALESCE(CAST(REPLACE(rrwv.revenue_recognition_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
      CAST(0 AS INT) AS revenue_recognition_time_key,
      rrwv.vin,
      rrwv.revenue_recognition_month as month,
      rrwv.revenue_recognition_year as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      -- Use original signs as they appear in NetSuite
      CAST(ROUND((mvr.total_amount / dp.deal_count) * 100) AS BIGINT) as amount_cents,
      CAST(mvr.total_amount / dp.deal_count AS DECIMAL(15,2)) as amount_dollars,
      'PERIOD_ALLOCATION' as allocation_method,
      CAST(1.0 / dp.deal_count AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields: PERIOD_ALLOCATION is derived, not a posted GL line
      'UNPOSTED' AS netsuite_posting_status
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN missing_vin_revenue mvr ON rrwv.revenue_recognition_period = mvr.revenue_recognition_period
    INNER JOIN deals_per_period dp ON rrwv.revenue_recognition_period = dp.revenue_recognition_period
    INNER JOIN account_mappings am ON mvr.account = am.account_id
    INNER JOIN bronze.ns.account a ON mvr.account = a.id
    WHERE dp.deal_count > 0
    GROUP BY rrwv.deal_id, mvr.account, rrwv.revenue_recognition_period, rrwv.vin, 
             rrwv.revenue_recognition_month, rrwv.revenue_recognition_year, 
             am.transaction_type, am.transaction_category, am.transaction_subcategory, 
             dp.deal_count, a.acctnumber, mvr.total_amount

    UNION ALL

    -- Missing VIN expense transactions (using actual transaction dates)
    SELECT
      CONCAT(dv.deal_id, '_', tl.expenseaccount, '_', t.id, '_MISSING_VIN_EXPENSE') as transaction_key,
      CAST(dv.deal_id AS STRING) as deal_key,
      CAST(tl.expenseaccount AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      dv.vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(tl.netamount * 100) AS BIGINT) as amount_cents,
      CAST(tl.netamount AS DECIMAL(15,2)) as amount_dollars,
      'MISSING_VIN_MATCH' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields
      CASE WHEN t.posting = 'T' THEN 'POSTED' ELSE 'UNPOSTED' END AS netsuite_posting_status
    FROM bronze.ns.transactionline tl
    INNER JOIN bronze.ns.transaction t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN deal_vins dv ON d.id = dv.deal_id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
        AND am.transaction_type IN ('EXPENSE', 'COST_OF_REVENUE', 'OTHER_EXPENSE')
        AND t.abbrevtype IN ('BILL', 'GENJRNL', 'BILLCRED', 'CC', 'CC CRED', 'CHK', 'ITEMSHIP', 'PAY CHK', 'DEP', 'ITEM RCP', 'INV WKST')  -- Include INV WKST for inventory adjustments
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND tl.netamount != 0
        AND am.is_direct_method_account = FALSE -- Exclude accounts handled by DIRECT method

    UNION ALL

    -- Orphaned VIN revenue transactions (VIN exists but not in our deal tables)
    SELECT
      CONCAT('ORPHANED_VIN_', t.id, '_', so.account, '_REVENUE') as transaction_key,
      NULL as deal_key,  -- Can't link to deal since VIN not in our tables
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
      -- Use original signs as they appear in NetSuite
      CAST(ROUND(SUM(so.amount) * 100) AS BIGINT) as amount_cents,
      CAST(SUM(so.amount) AS DECIMAL(15,2)) as amount_dollars,
      'ORPHANED_VIN' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields
      CASE WHEN MAX(t.posting) = 'T' THEN 'POSTED' ELSE 'UNPOSTED' END AS netsuite_posting_status
    FROM bronze.ns.salesinvoiced so
    INNER JOIN bronze.ns.transaction t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND t.custbody_leaseend_vinno NOT LIKE '%,%'  -- Single VIN only
        AND (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)  -- No deal ID
        AND t.abbrevtype IN ('SALESORD','CREDITMEMO','CREDMEM','INV','GENJRNL','BILL','BILLCRED','CC','INV WKST')
        AND am.transaction_type = 'REVENUE'
        AND am.transaction_subcategory != 'CHARGEBACK'
        AND t.trandate IS NOT NULL
        AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
        AND (t.posting = 'T' OR t.posting IS NULL)  -- TYPE A FIX: Only include posted transactions
        AND so.amount != 0
        AND am.is_direct_method_account = FALSE
        -- Only include VINs that don't exist in our deal lookup tables
        AND NOT EXISTS (
          SELECT 1 FROM bronze.leaseend_db_public.cars c 
          WHERE UPPER(c.vin) = UPPER(t.custbody_leaseend_vinno)
            AND c.vin IS NOT NULL AND LENGTH(c.vin) = 17
            AND (c._fivetran_deleted = FALSE OR c._fivetran_deleted IS NULL)
        )
        AND NOT EXISTS (
          SELECT 1 FROM silver.finance.bridge_vin_deal bd
          WHERE UPPER(bd.vin) = UPPER(t.custbody_leaseend_vinno)
            AND bd.is_active = TRUE AND bd.vin IS NOT NULL AND LENGTH(bd.vin) = 17
        )
        -- Ensure a posted GL line exists for the same transaction/account to match GL
        AND EXISTS (
          SELECT 1 FROM bronze.ns.transactionaccountingline tal
          WHERE tal.transaction = t.id AND tal.account = so.account AND tal.posting = 'T'
            AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
        )
    GROUP BY t.id, so.account, t.trandate, t.custbody_leaseend_vinno, am.transaction_type, am.transaction_category, am.transaction_subcategory

    UNION ALL

    -- Unallocated revenue transactions (no deal_id)
    SELECT
      CONCAT('UNALLOCATED_', ur.account, '_', ur.transaction_period, '_REVENUE') as transaction_key,
      NULL as deal_key,
      CAST(ur.account AS STRING) as account_key,
      COALESCE(CAST(REPLACE(ur.transaction_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
      CAST(0 AS BIGINT) AS netsuite_posting_time_key,
      COALESCE(CAST(REPLACE(ur.transaction_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
      CAST(0 AS INT) AS revenue_recognition_time_key,
      NULL as vin,
      MONTH(TO_DATE(ur.transaction_period || '-01')) as month,
      YEAR(TO_DATE(ur.transaction_period || '-01')) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      -- Use original signs as they appear in NetSuite
      CAST(ROUND(ur.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(ur.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'UNALLOCATED' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.salesinvoiced' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields  
      'POSTED' AS netsuite_posting_status  -- UNALLOCATED transactions are filtered to posted only
    FROM unallocated_revenue ur
    INNER JOIN account_mappings am ON ur.account = am.account_id
    INNER JOIN bronze.ns.account a ON ur.account = a.id
    GROUP BY ur.account, ur.transaction_period, am.transaction_type, am.transaction_category, 
             am.transaction_subcategory, a.acctnumber, ur.total_amount

    UNION ALL

    -- Unallocated expense transactions (no deal_id)
    SELECT
      CONCAT('UNALLOCATED_', uec.account, '_', uec.transaction_period, '_EXPENSE') as transaction_key,
      NULL as deal_key,
      CAST(uec.account AS STRING) as account_key,
      COALESCE(CAST(REPLACE(uec.transaction_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
      CAST(0 AS BIGINT) AS netsuite_posting_time_key,
      COALESCE(CAST(REPLACE(uec.transaction_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
      CAST(0 AS INT) AS revenue_recognition_time_key,
      NULL as vin,
      MONTH(TO_DATE(uec.transaction_period || '-01')) as month,
      YEAR(TO_DATE(uec.transaction_period || '-01')) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND(uec.total_amount * 100) AS BIGINT) as amount_cents,
      CAST(uec.total_amount AS DECIMAL(15,2)) as amount_dollars,
      'UNALLOCATED' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      -- Posting status fields
      'POSTED' AS netsuite_posting_status  -- UNALLOCATED expenses are filtered to posted only
    FROM unallocated_expenses_consolidated uec
    INNER JOIN account_mappings am ON uec.account = am.account_id

    UNION ALL

    -- GL-only expense transactions (no transactionline representation) - line level
    SELECT
      CONCAT('GL_ONLY_EXP_', tal.account, '_', t.id, '_', DATE_FORMAT(t.trandate, 'yyyyMMdd'), '_', CAST(ROUND(tal.amount * 100) AS BIGINT)) as transaction_key,
      NULL as deal_key,
      CAST(tal.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(t.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      NULL as vin,
      MONTH(t.trandate) as month,
      YEAR(t.trandate) as year,
      am.transaction_type,
      am.transaction_category,
      am.transaction_subcategory,
      CAST(ROUND((CASE 
                    WHEN CAST(REGEXP_EXTRACT(aexp.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
                      THEN -tal.amount 
                    ELSE tal.amount 
                  END) * 100) AS BIGINT) as amount_cents,
      CAST(CASE 
            WHEN CAST(REGEXP_EXTRACT(aexp.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
              THEN -tal.amount 
            ELSE tal.amount 
          END AS DECIMAL(15,2)) as amount_dollars,
      'GL_ONLY_EXPENSE' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      'POSTED' AS netsuite_posting_status
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
    INNER JOIN bronze.ns.account aexp ON tal.account = aexp.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    WHERE tal.posting = 'T'
      AND aexp.acctnumber IN ('5510','6100','6200','6040','5402','5401','5404','5141','6510','7140','5210')
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND tal.amount != 0
      AND NOT EXISTS (
        SELECT 1 FROM bronze.ns.transactionline tl2
        WHERE tl2.transaction = t.id AND tl2.expenseaccount = tal.account
          AND t.abbrevtype NOT IN ('ITEMSHIP', 'ITEM RCP')  -- Allow inventory transactions through
      )

    UNION ALL

    -- GL volume bonus transactions (line-level to align with GL totals and counts)
    SELECT
      CONCAT('GL_VOLUME_BONUS_', nvvb.account, '_', nvvb.transaction_id, '_', nvvb.gl_line_id, '_', DATE_FORMAT(nvvb.trandate, 'yyyyMMdd'), '_', CAST(ROUND(nvvb.gl_amount * 100) AS BIGINT)) as transaction_key,
      NULL as deal_key,
      CAST(nvvb.account AS STRING) as account_key,
      COALESCE(CAST(DATE_FORMAT(nvvb.trandate, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
      COALESCE(CAST(DATE_FORMAT(nvvb.trandate, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
      COALESCE(CAST(DATE_FORMAT(nvvb.trandate, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
      COALESCE(CAST(DATE_FORMAT(nvvb.trandate, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
      NULL as vin,
      nvvb.transaction_month as month,
      nvvb.transaction_year as year,
      'REVENUE' as transaction_type,
      CASE WHEN nvvb.acctnumber LIKE '4110%' THEN 'VSC' ELSE 'GAP' END as transaction_category,
      'VOLUME_BONUS' as transaction_subcategory,
      CAST(ROUND((CASE 
                    WHEN CAST(REGEXP_EXTRACT(nvvb.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
                      THEN -nvvb.gl_amount 
                    ELSE nvvb.gl_amount 
                  END) * 100) AS BIGINT) as amount_cents,
      CAST(CASE 
            WHEN CAST(REGEXP_EXTRACT(nvvb.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
              THEN -nvvb.gl_amount 
            ELSE nvvb.gl_amount 
          END AS DECIMAL(15,2)) as amount_dollars,
      'GL_VOLUME_BONUS' as allocation_method,
      CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
      'bronze.ns.transactionaccountingline' as _source_table,
      FALSE AS has_credit_memo,
      CAST(NULL AS INT) AS credit_memo_date_key,
      CAST(NULL AS INT) AS credit_memo_time_key,
      'POSTED' AS netsuite_posting_status
    FROM non_vin_volume_bonus_gl nvvb


  ),
  
  final_transactions_with_account_info AS (
    SELECT
        ftb.*,
        acc_ft.acctnumber,
        ROW_NUMBER() OVER (ORDER BY ftb.transaction_key) AS global_rownum,
        CONCAT(ftb.transaction_key, '_', ROW_NUMBER() OVER (ORDER BY ftb.transaction_key)) AS transaction_key_unique
    FROM final_transactions_base ftb
    LEFT JOIN bronze.ns.account acc_ft ON ftb.account_key = CAST(acc_ft.id AS STRING)
  ),
  
  -- Driver count deals: Calculate net activity per deal per month following NetSuite business rules
  -- Rule: Count deals in the month transactions occur (positive = +1, negative = -1)
  monthly_driver_net_activity AS (
    SELECT 
      ftb.deal_key,
      ftb.year,
      ftb.month,
      SUM(ftb.amount_dollars) as net_amount
    FROM final_transactions_base ftb
    INNER JOIN bronze.ns.account a ON ftb.account_key = CAST(a.id AS STRING)
    WHERE ftb.deal_key IS NOT NULL
      AND ftb.transaction_type = 'REVENUE'
      AND ftb.amount_dollars != 0
      AND (
        -- Jan 2025 onwards: Count deals with 4141 titling fees
        (ftb.year >= 2025 AND a.acctnumber = '4141')
        OR
        -- Before 2025: Count deals with 4130 doc fees  
        (ftb.year < 2025 AND a.acctnumber = '4130')
      )
    GROUP BY ftb.deal_key, ftb.year, ftb.month
  ),
  
  -- Driver count transactions: Mark transactions from deals that contribute to driver metrics  
  -- NetSuite rule: Count deals with net activity in each month (positive = +1, negative = -1)
  driver_count_eligible_deals AS (
    SELECT DISTINCT
      deal_key,
      year,
      month
    FROM monthly_driver_net_activity
    WHERE net_amount != 0  -- Include all deals with non-zero net activity per month
  ),
  
  -- Ensure only ONE transaction per deal per month gets the driver count flag
  driver_count_transactions_ranked AS (
    SELECT
      ftwai.transaction_key_unique,
      ftwai.deal_key,
      ftwai.year,
      ftwai.month,
      a.acctnumber,
      ROW_NUMBER() OVER (
        PARTITION BY ftwai.deal_key, ftwai.year, ftwai.month, a.acctnumber
        ORDER BY ftwai.global_rownum
      ) AS transaction_rank
    FROM final_transactions_with_account_info ftwai
    INNER JOIN driver_count_eligible_deals dced 
      ON ftwai.deal_key = dced.deal_key 
      AND ftwai.year = dced.year 
      AND ftwai.month = dced.month
    INNER JOIN bronze.ns.account a ON ftwai.account_key = CAST(a.id AS STRING)
    WHERE ftwai.transaction_type = 'REVENUE'
      AND ftwai.amount_dollars != 0
      AND (
        -- Jan 2025 onwards: 4141 titling fees
        (ftwai.year >= 2025 AND a.acctnumber = '4141')
        OR
        -- Before 2025: 4130 doc fees
        (ftwai.year < 2025 AND a.acctnumber = '4130')
      )
  ),
  
  driver_count_transactions AS (
    SELECT
      transaction_key_unique,
      TRUE as should_count_for_drivers
    FROM driver_count_transactions_ranked
    WHERE transaction_rank = 1  -- Only the first transaction per deal/month/account gets the flag
  )
  
  -- Add driver count calculation and final columns
  SELECT
    ftwai.transaction_key,
    ftwai.deal_key,
    ftwai.account_key,
    ftwai.netsuite_posting_date_key,
    ftwai.netsuite_posting_time_key,
    ftwai.revenue_recognition_date_key,
    ftwai.revenue_recognition_time_key,
    ftwai.has_credit_memo,
    ftwai.credit_memo_date_key,
    ftwai.credit_memo_time_key,
    ftwai.netsuite_posting_status,
    ftwai.vin,
    ftwai.month,
    ftwai.year,
    ftwai.transaction_type,
    ftwai.transaction_category,
    ftwai.transaction_subcategory,
    CASE 
        WHEN am.transaction_type IN ('COST_OF_REVENUE', 'EXPENSE', 'OTHER_EXPENSE') 
        THEN ftwai.amount_cents  -- Keep expense accounts positive  
        ELSE ftwai.amount_cents  -- FIXED: Preserve original NetSuite signs for revenue accounts including chargebacks
    END as amount_cents,
    CASE 
        WHEN am.transaction_type IN ('COST_OF_REVENUE', 'EXPENSE', 'OTHER_EXPENSE') 
        THEN ftwai.amount_dollars -- Keep expense accounts positive
        ELSE ftwai.amount_dollars -- FIXED: Preserve original NetSuite signs for revenue accounts including chargebacks
    END as amount_dollars,
    ftwai.allocation_method,
    ftwai.allocation_factor,
    -- Driver count logic: TRUE for qualifying fee transactions in deals that count toward driver metrics
    COALESCE(dct.should_count_for_drivers, FALSE) AS is_driver_count,
    ftwai._source_table,
    current_timestamp() AS _load_timestamp
  FROM final_transactions_with_account_info ftwai
  LEFT JOIN driver_count_transactions dct
      ON ftwai.transaction_key_unique = dct.transaction_key_unique
  LEFT JOIN account_mappings am ON ftwai.account_key = CAST(am.account_id AS STRING)

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
    target.has_credit_memo = source.has_credit_memo,
    target.credit_memo_date_key = source.credit_memo_date_key,
    target.credit_memo_time_key = source.credit_memo_time_key,
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
    target.is_driver_count = source.is_driver_count,
    target.netsuite_posting_status = source.netsuite_posting_status,
    target._source_table = source._source_table,
    target._load_timestamp = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
    transaction_key,
    deal_key,
    account_key,
    netsuite_posting_date_key,
    netsuite_posting_time_key,
    revenue_recognition_date_key,
    revenue_recognition_time_key,
    has_credit_memo,
    credit_memo_date_key,
    credit_memo_time_key,
    vin,
    month,
    year,
    transaction_type,
    transaction_category,
    transaction_subcategory,
    amount_cents,
    amount_dollars,
    allocation_method,
    allocation_factor,
    is_driver_count,
    netsuite_posting_status,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.transaction_key,
    source.deal_key,
    source.account_key,
    source.netsuite_posting_date_key,
    source.netsuite_posting_time_key,
    source.revenue_recognition_date_key,
    source.revenue_recognition_time_key,
    source.has_credit_memo,
    source.credit_memo_date_key,
    source.credit_memo_time_key,
    source.vin,
    source.month,
    source.year,
    source.transaction_type,
    source.transaction_category,
    source.transaction_subcategory,
    source.amount_cents,
    source.amount_dollars,
    source.allocation_method,
    source.allocation_factor,
    source.is_driver_count,
    source.netsuite_posting_status,
    source._source_table,
    source._load_timestamp
  );
-- Optimize the table to compact small files
OPTIMIZE silver.finance.fact_deal_netsuite_transactions_base;

-- 3. Create the backward-compatible view that filters to posted transactions only
-- First drop the existing table (since we're converting to a base table + view architecture)
DROP TABLE IF EXISTS silver.finance.fact_deal_netsuite_transactions;

CREATE VIEW silver.finance.fact_deal_netsuite_transactions AS
SELECT 
  transaction_key,
  deal_key,
  account_key,
  netsuite_posting_date_key,
  netsuite_posting_time_key,
  revenue_recognition_date_key,
  revenue_recognition_time_key,
  has_credit_memo,
  credit_memo_date_key,
  credit_memo_time_key,
  vin,
  month,
  year,
  transaction_type,
  transaction_category,
  transaction_subcategory,
  amount_cents,
  amount_dollars,
  allocation_method,
  allocation_factor,
  is_driver_count,
  _source_table,
  _load_timestamp
FROM silver.finance.fact_deal_netsuite_transactions_base
WHERE netsuite_posting_status = 'POSTED'  -- Only include posted transactions for backward compatibility
;