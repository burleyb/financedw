-- models/silver/fact/fact_deal_netsuite_transactions.sql
-- Normalized NetSuite fact table with account foreign keys for flexible pivoting

-- Drop and recreate table to ensure correct schema
DROP TABLE IF EXISTS silver.finance.fact_deal_netsuite_transactions;

-- 1. Create the normalized fact table
CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_netsuite_transactions (
  transaction_key STRING NOT NULL, -- Unique key: deal_key + account_key + transaction_type
  deal_key STRING NOT NULL,
  account_key STRING NOT NULL, -- Foreign key to dim_account (account_id as string)
  netsuite_posting_date_key BIGINT,
  netsuite_posting_time_key BIGINT,
  revenue_recognition_date_key INT,
  revenue_recognition_time_key INT,
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
        ds.deal_id,
        ds.updated_date_utc as revenue_recognition_date_utc,
        from_utc_timestamp(ds.updated_date_utc, 'America/Denver') as revenue_recognition_date_mt,
        DATE_FORMAT(from_utc_timestamp(ds.updated_date_utc, 'America/Denver'), 'yyyy-MM') as revenue_recognition_period,
        YEAR(from_utc_timestamp(ds.updated_date_utc, 'America/Denver')) as revenue_recognition_year,
        MONTH(from_utc_timestamp(ds.updated_date_utc, 'America/Denver')) as revenue_recognition_month,
        ROW_NUMBER() OVER (PARTITION BY ds.deal_id ORDER BY ds.updated_date_utc ASC) as rn
    FROM bronze.leaseend_db_public.deal_states ds
    WHERE ds.state = 'funded' 
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
        frr.revenue_recognition_date_mt,
        frr.revenue_recognition_period,
        frr.revenue_recognition_year,
        frr.revenue_recognition_month,
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
      -- These are ONLY overrides for specific business categorization
      -- NOT restrictions - all other accounts will get auto-classified
      SELECT * FROM VALUES
        -- Revenue accounts with specific business meaning
        (236, 'REVENUE', 'RESERVE', 'BASE'),
        (474, 'REVENUE', 'RESERVE', 'BONUS'),
        (524, 'REVENUE', 'RESERVE', 'CHARGEBACK'),
        (237, 'REVENUE', 'VSC', 'BASE'),
        (546, 'REVENUE', 'VSC', 'ADVANCE'),
        (479, 'REVENUE', 'VSC', 'BONUS'),
        (545, 'REVENUE', 'VSC', 'COST'),
        (544, 'REVENUE', 'VSC', 'CHARGEBACK'),
        (238, 'REVENUE', 'GAP', 'BASE'),
        (547, 'REVENUE', 'GAP', 'ADVANCE'),
        (480, 'REVENUE', 'GAP', 'VOLUME_BONUS'),
        (548, 'REVENUE', 'GAP', 'COST'),
        (549, 'REVENUE', 'GAP', 'CHARGEBACK'),
        (239, 'REVENUE', 'DOC_FEES', 'BASE'),
        (517, 'REVENUE', 'DOC_FEES', 'CHARGEBACK'),
        (451, 'REVENUE', 'TITLING_FEES', 'BASE'),
        (563, 'REVENUE', 'REBATES', 'DISCOUNT'),
        
        -- Expense accounts with specific business meaning
        (528, 'EXPENSE', 'PEOPLE_COST', 'FUNDING_CLERKS'),
        (552, 'EXPENSE', 'PEOPLE_COST', 'COMMISSION'),
        (532, 'EXPENSE', 'VARIANCE', 'PAYOFF'),
        (533, 'EXPENSE', 'VARIANCE', 'SALES_TAX'),
        (534, 'EXPENSE', 'VARIANCE', 'REGISTRATION'),
        (538, 'EXPENSE', 'VARIANCE', 'CUSTOMER_EXPERIENCE'),
        (539, 'EXPENSE', 'VARIANCE', 'PENALTIES'),
        (447, 'EXPENSE', 'OTHER_COR', 'BANK_BUYOUT_FEES'),
        (267, 'EXPENSE', 'VSC_COR', 'BASE'),
        (498, 'EXPENSE', 'VSC_COR', 'ADVANCE'),
        (269, 'EXPENSE', 'GAP_COR', 'BASE'),
        (499, 'EXPENSE', 'GAP_COR', 'ADVANCE'),
        (452, 'EXPENSE', 'TITLING_FEES', 'BASE'),
        (551, 'EXPENSE', 'REPO', 'BASE')
      AS t(account_id, transaction_type, transaction_category, transaction_subcategory)
    )
    
    -- Include ALL transactional accounts with appropriate classification
    SELECT 
      ata.account_id,
      -- Use business override if exists, otherwise auto-classify based on NetSuite account type
      COALESCE(
        bao.transaction_type,
        CASE 
          WHEN UPPER(a.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME', 'OTHINCOME') THEN 'REVENUE'
          WHEN UPPER(a.accttype) IN ('EXPENSE', 'COGS', 'COSTOFGOODSSOLD', 'OTHEREXPENSE', 'OTHEXPENSE') THEN 'EXPENSE'
          WHEN UPPER(a.accttype) IN ('BANK', 'ACCREC', 'INVENTORY', 'OTHCURRASSET', 'FIXEDASSET', 'ACCUMDEPRECIATION', 'OTHERASSET') THEN 'ASSET'
          WHEN UPPER(a.accttype) IN ('ACCTSPAY', 'CREDITCARD', 'OTHCURRLIAB', 'LONGTERMLIAB') THEN 'LIABILITY'
          WHEN UPPER(a.accttype) IN ('EQUITY', 'RETEARNINGS') THEN 'EQUITY'
          ELSE 'OTHER'
        END
      ) as transaction_type,
      -- Use business category if exists, otherwise auto-assign based on account type
      COALESCE(
        bao.transaction_category,
        CASE 
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
  
  -- VIN-matching transactions
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
        AND am.transaction_type = 'REVENUE'
    GROUP BY UPPER(t.custbody_leaseend_vinno), so.account
  ),
  
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
        AND am.transaction_type = 'EXPENSE'
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY UPPER(t.custbody_leaseend_vinno), tl.expenseaccount
  ),
  
  -- VIN-matching transactions from transactionaccountingline (captures many accounts not in other tables)
  vin_matching_accounting AS (
    SELECT
        UPPER(t.custbody_leaseend_vinno) as vin,
        tal.account,
        SUM(tal.amount) AS total_amount
    FROM bronze.ns.transactionaccountingline AS tal
    INNER JOIN bronze.ns.transaction AS t ON tal.transaction = t.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    WHERE LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno IS NOT NULL
        AND tal.amount != 0
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY UPPER(t.custbody_leaseend_vinno), tal.account
  ),
  

  
  -- Missing VIN transactions by period (for allocation)
  missing_vin_revenue AS (
    SELECT
        rrwv.revenue_recognition_period,
        so.account,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    INNER JOIN account_mappings am ON so.account = am.account_id
    WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
        AND am.transaction_type = 'REVENUE'
    GROUP BY rrwv.revenue_recognition_period, so.account
  ),
  
  missing_vin_expenses AS (
    SELECT
        rrwv.revenue_recognition_period,
        tl.expenseaccount as account,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
        AND am.transaction_type = 'EXPENSE'
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY rrwv.revenue_recognition_period, tl.expenseaccount
  ),
  
  -- Missing VIN transactions from transactionaccountingline
  missing_vin_accounting AS (
    SELECT
        rrwv.revenue_recognition_period,
        tal.account,
        SUM(tal.amount) AS total_amount
    FROM bronze.ns.transactionaccountingline AS tal
    INNER JOIN bronze.ns.transaction AS t ON tal.transaction = t.id
    INNER JOIN bronze.leaseend_db_public.deals d ON t.custbody_le_deal_id = d.id
    INNER JOIN revenue_recognition_with_vins rrwv ON d.id = rrwv.deal_id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.custbody_le_deal_id IS NOT NULL
        AND tal.amount != 0
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY rrwv.revenue_recognition_period, tal.account
  ),
  
  -- Unallocated transactions (not tied to any deal)
  unallocated_revenue AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        so.account,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    WHERE (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
        AND am.transaction_type = 'REVENUE'
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), so.account
  ),
  
  unallocated_expenses AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        tl.expenseaccount as account,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
        AND am.transaction_type = 'EXPENSE'
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), tl.expenseaccount
  ),
  
  unallocated_accounting AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        tal.account,
        SUM(tal.amount) AS total_amount
    FROM bronze.ns.transactionaccountingline AS tal
    INNER JOIN bronze.ns.transaction AS t ON tal.transaction = t.id
    INNER JOIN account_mappings am ON tal.account = am.account_id
    WHERE (t.custbody_le_deal_id IS NULL OR t.custbody_le_deal_id = 0)
        AND (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
        AND tal.amount != 0
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), tal.account
  ),
  
  -- Combine VIN-matching and allocated amounts
  final_transactions AS (
    -- VIN-matching revenue transactions
    SELECT
        CONCAT(rrwv.deal_id, '_', vmr.account, '_REVENUE') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(vmr.account AS STRING) as account_key, -- Store account_id as string for FK to dim_account
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
        rrwv.vin,
        rrwv.revenue_recognition_month as month,
        rrwv.revenue_recognition_year as year,
        am.transaction_type,
        am.transaction_category,
        am.transaction_subcategory,
        CAST(ROUND(vmr.total_amount * 100) AS BIGINT) as amount_cents,
        CAST(vmr.total_amount AS DECIMAL(15,2)) as amount_dollars,
        'VIN_MATCH' as allocation_method,
        CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
        'bronze.ns.salesinvoiced' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN vin_matching_revenue vmr ON rrwv.vin = vmr.vin
    INNER JOIN account_mappings am ON vmr.account = am.account_id
    
    UNION ALL
    
    -- VIN-matching expense transactions
    SELECT
        CONCAT(rrwv.deal_id, '_', vme.account, '_EXPENSE') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(vme.account AS STRING) as account_key, -- Store account_id as string for FK to dim_account
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
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
        'bronze.ns.transactionline' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN vin_matching_expenses vme ON rrwv.vin = vme.vin
    INNER JOIN account_mappings am ON vme.account = am.account_id
    
    UNION ALL
    
    -- Allocated missing VIN revenue transactions
    SELECT
        CONCAT(rrwv.deal_id, '_', mvr.account, '_REVENUE_ALLOCATED') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(mvr.account AS STRING) as account_key, -- Store account_id as string for FK to dim_account
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
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
        'bronze.ns.salesinvoiced' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    INNER JOIN missing_vin_revenue mvr ON rrwv.revenue_recognition_period = mvr.revenue_recognition_period
    INNER JOIN account_mappings am ON mvr.account = am.account_id
    
    UNION ALL
    
    -- Allocated missing VIN expense transactions
    SELECT
        CONCAT(rrwv.deal_id, '_', mve.account, '_EXPENSE_ALLOCATED') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(mve.account AS STRING) as account_key, -- Store account_id as string for FK to dim_account
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
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
        'bronze.ns.transactionline' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    INNER JOIN missing_vin_expenses mve ON rrwv.revenue_recognition_period = mve.revenue_recognition_period
    INNER JOIN account_mappings am ON mve.account = am.account_id
    
    UNION ALL
    
    -- VIN-matching transactions from transactionaccountingline
    SELECT
        CONCAT(rrwv.deal_id, '_', vma.account, '_ACCOUNTING') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(vma.account AS STRING) as account_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
        rrwv.vin,
        rrwv.revenue_recognition_month as month,
        rrwv.revenue_recognition_year as year,
        am.transaction_type,
        am.transaction_category,
        am.transaction_subcategory,
        CAST(ROUND(vma.total_amount * 100) AS BIGINT) as amount_cents,
        CAST(vma.total_amount AS DECIMAL(15,2)) as amount_dollars,
        'VIN_MATCH' as allocation_method,
        CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
        'bronze.ns.transactionaccountingline' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN vin_matching_accounting vma ON rrwv.vin = vma.vin
    INNER JOIN account_mappings am ON vma.account = am.account_id
    

    
    UNION ALL
    
    -- Allocated missing VIN transactions from transactionaccountingline
    SELECT
        CONCAT(rrwv.deal_id, '_', mva.account, '_ACCOUNTING_ALLOCATED') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(mva.account AS STRING) as account_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_mt, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
        rrwv.vin,
        rrwv.revenue_recognition_month as month,
        rrwv.revenue_recognition_year as year,
        am.transaction_type,
        am.transaction_category,
        am.transaction_subcategory,
        CAST(ROUND((mva.total_amount / NULLIF(dpp.deal_count, 0)) * 100) AS BIGINT) as amount_cents,
        CAST(mva.total_amount / NULLIF(dpp.deal_count, 0) AS DECIMAL(15,2)) as amount_dollars,
        'PERIOD_ALLOCATION' as allocation_method,
        CAST(1.0 / NULLIF(dpp.deal_count, 0) AS DECIMAL(10,6)) as allocation_factor,
        'bronze.ns.transactionaccountingline' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    INNER JOIN missing_vin_accounting mva ON rrwv.revenue_recognition_period = mva.revenue_recognition_period
    INNER JOIN account_mappings am ON mva.account = am.account_id
    
    UNION ALL
    
    -- Unallocated transactions (not tied to any deal)
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
        'bronze.ns.salesinvoiced' as _source_table
    FROM unallocated_revenue
    
    UNION ALL
    
    -- Unallocated transactions (not tied to any deal)
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
        'EXPENSE' as transaction_type,
        'GENERAL_EXPENSE' as transaction_category,
        'STANDARD' as transaction_subcategory,
        CAST(ROUND(total_amount * 100) AS BIGINT) as amount_cents,
        CAST(total_amount AS DECIMAL(15,2)) as amount_dollars,
        'UNALLOCATED' as allocation_method,
        CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
        'bronze.ns.transactionline' as _source_table
    FROM unallocated_expenses
    
    UNION ALL
    
    -- Unallocated transactions (not tied to any deal)
    SELECT
        CONCAT(transaction_period, '_', account, '_ACCOUNTING_UNALLOCATED') as transaction_key,
        'UNALLOCATED' as deal_key,
        CAST(account AS STRING) as account_key,
        COALESCE(CAST(REPLACE(transaction_period, '-', '') || '01' AS BIGINT), 0) AS netsuite_posting_date_key,
        CAST(0 AS BIGINT) AS netsuite_posting_time_key,
        COALESCE(CAST(REPLACE(transaction_period, '-', '') || '01' AS INT), 0) AS revenue_recognition_date_key,
        CAST(0 AS INT) AS revenue_recognition_time_key,
        'UNALLOCATED' as vin,
        CAST(SPLIT(transaction_period, '-')[1] AS INT) as month,
        CAST(SPLIT(transaction_period, '-')[0] AS INT) as year,
        'OTHER' as transaction_type,
        'UNMAPPED' as transaction_category,
        'STANDARD' as transaction_subcategory,
        CAST(ROUND(total_amount * 100) AS BIGINT) as amount_cents,
        CAST(total_amount AS DECIMAL(15,2)) as amount_dollars,
        'UNALLOCATED' as allocation_method,
        CAST(1.0 AS DECIMAL(10,6)) as allocation_factor,
        'bronze.ns.transactionaccountingline' as _source_table
    FROM unallocated_accounting
  )
  
  SELECT 
    ft.*,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM final_transactions ft

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
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    transaction_key, deal_key, account_key, netsuite_posting_date_key, netsuite_posting_time_key,
    revenue_recognition_date_key, revenue_recognition_time_key, vin, month, year,
    transaction_type, transaction_category, transaction_subcategory, amount_cents, amount_dollars,
    allocation_method, allocation_factor, _source_table, _load_timestamp
  )
  VALUES (
    source.transaction_key, source.deal_key, source.account_key, source.netsuite_posting_date_key, source.netsuite_posting_time_key,
    source.revenue_recognition_date_key, source.revenue_recognition_time_key, source.vin, source.month, source.year,
    source.transaction_type, source.transaction_category, source.transaction_subcategory, source.amount_cents, source.amount_dollars,
    source.allocation_method, source.allocation_factor, source._source_table, source._load_timestamp
  ); 