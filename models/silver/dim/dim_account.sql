-- models/silver/dim/dim_account.sql
-- Enhanced Account Dimension with DYNAMIC Income Statement Groupings
-- This version automatically classifies accounts based on NetSuite hierarchy and patterns

-- Drop and recreate table to ensure correct schema
DROP TABLE IF EXISTS silver.finance.dim_account;

CREATE TABLE IF NOT EXISTS silver.finance.dim_account (
  account_key STRING NOT NULL, -- Natural key (account ID)
  account_id BIGINT, -- Original NetSuite account ID
  account_number STRING, -- Account number
  account_name STRING, -- Account display name
  account_full_name STRING, -- Full hierarchical name
  account_type STRING, -- Account type
  account_category STRING, -- Business category classification
  
  -- New transaction classification fields
  transaction_type STRING, -- REVENUE, COST_OF_REVENUE, EXPENSE, OTHER_INCOME, OTHER_EXPENSE, ASSET, LIABILITY, EQUITY
  transaction_category STRING, -- RESERVE, VSC, GAP, DOC_FEES, DIRECT_PEOPLE_COST, etc.
  transaction_subcategory STRING, -- BASE, ADVANCE, BONUS, CHARGEBACK, etc.
  
  -- Income Statement Metric Groupings
  revenue_metric_group STRING, -- REV_RESERVE, REV_VSC, REV_GAP, REV_DOC_FEES, etc.
  cost_metric_group STRING, -- COR_DIRECT_PEOPLE, COR_PAYOFF_EXPENSE, COR_OTHER, etc.
  expense_metric_group STRING, -- PEOPLE_COST, MARKETING, GA_EXPENSE, etc.
  other_metric_group STRING, -- OTHER_INCOME, OTHER_EXPENSE, etc.
  
  -- High-level P&L Groupings
  is_total_revenue BOOLEAN, -- Part of total revenue calculation
  is_cost_of_revenue BOOLEAN, -- Part of cost of revenue calculation
  is_gross_profit BOOLEAN, -- Part of gross profit calculation (revenue - COR)
  is_operating_expense BOOLEAN, -- Part of operating expense calculation
  is_net_ordinary_revenue BOOLEAN, -- Part of net ordinary revenue calculation
  is_other_income_expense BOOLEAN, -- Part of other income/expense calculation
  is_net_income BOOLEAN, -- Part of net income calculation
  
  parent_account_key STRING, -- Parent account key for joins (STRING)
  is_summary BOOLEAN, -- Whether this is a summary account
  is_inactive BOOLEAN, -- Whether the account is inactive
  is_inventory BOOLEAN, -- Whether this is an inventory account
  description STRING, -- Account description
  subsidiary STRING, -- Subsidiary information
  include_children STRING, -- Include children flag
  eliminate STRING, -- Elimination flag
  revalue STRING, -- Revalue flag
  reconcile_with_matching STRING, -- Reconcile with matching flag
  bank_name STRING, -- Bank name (if applicable)
  bank_routing_number STRING, -- Bank routing number (if applicable)
  special_account STRING, -- Special account designation
  external_id STRING, -- External ID
  _source_table STRING, -- Source table name
  _load_timestamp TIMESTAMP -- Load timestamp
)
USING DELTA
COMMENT 'Silver layer NetSuite accounts dimension with DYNAMIC classification'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes with DYNAMIC CLASSIFICATION
MERGE INTO silver.finance.dim_account AS target
USING (
  WITH 
  -- Dynamic parent account analysis to understand account hierarchy
  account_hierarchy AS (
    SELECT 
      a.id,
      a.acctnumber,
      a.fullname,
      a.accttype,
      a.parent,
      a.accountsearchdisplayname,
      a.description,
      a.issummary,
      a.isinactive,
      a.inventory,
      a.subsidiary,
      a.includechildren,
      a.eliminate,
      a.revalue,
      a.reconcilewithmatching,
      a.sbankname,
      a.sbankroutingnumber,
      a.sspecacct,
      a.externalid,
      a.lastmodifieddate,
      p.acctnumber as parent_account_number,
      p.fullname as parent_account_name,
      p.accttype as parent_account_type
    FROM bronze.ns.account a
    LEFT JOIN bronze.ns.account p ON a.parent = p.id AND p._fivetran_deleted = FALSE
    WHERE a._fivetran_deleted = FALSE
  ),
  
  -- Dynamic classification based on account patterns and hierarchy
  dynamic_classification AS (
    SELECT 
      ah.*,
      
      -- DYNAMIC TRANSACTION TYPE CLASSIFICATION
      CASE 
        -- Revenue accounts: 4000 series OR parent is revenue OR account type indicates revenue
        WHEN ah.acctnumber BETWEEN '4000' AND '4999' THEN 'REVENUE'
        WHEN ah.parent_account_number BETWEEN '4000' AND '4999' THEN 'REVENUE'
        WHEN UPPER(ah.accttype) IN ('INCOME', 'REVENUE') THEN 'REVENUE'
        WHEN UPPER(ah.parent_account_type) IN ('INCOME', 'REVENUE') THEN 'REVENUE'
        WHEN UPPER(ah.fullname) LIKE '%REVENUE%' OR UPPER(ah.fullname) LIKE '%INCOME%' THEN 'REVENUE'
        
        -- Cost of Revenue accounts: 5000 series OR parent is COR OR COGS type
        WHEN ah.acctnumber BETWEEN '5000' AND '5999' THEN 'COST_OF_REVENUE'
        WHEN ah.parent_account_number BETWEEN '5000' AND '5999' THEN 'COST_OF_REVENUE'
        WHEN UPPER(ah.accttype) IN ('COGS', 'COSTOFGOODSSOLD') THEN 'COST_OF_REVENUE'
        WHEN UPPER(ah.parent_account_type) IN ('COGS', 'COSTOFGOODSSOLD') THEN 'COST_OF_REVENUE'
        WHEN UPPER(ah.fullname) LIKE '%COST OF REVENUE%' OR UPPER(ah.fullname) LIKE '%COGS%' THEN 'COST_OF_REVENUE'
        
        -- Operating Expenses: 6000-7999 series OR expense type
        WHEN ah.acctnumber BETWEEN '6000' AND '7999' THEN 'EXPENSE'
        WHEN ah.parent_account_number BETWEEN '6000' AND '7999' THEN 'EXPENSE'
        WHEN UPPER(ah.accttype) = 'EXPENSE' THEN 'EXPENSE'
        WHEN UPPER(ah.parent_account_type) = 'EXPENSE' THEN 'EXPENSE'
        
        -- Other Income: 9000+ series with income type OR other income type
        WHEN ah.acctnumber >= '9000' AND UPPER(ah.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME', 'OTHINCOME') THEN 'OTHER_INCOME'
        WHEN ah.parent_account_number >= '9000' AND UPPER(ah.parent_account_type) IN ('INCOME', 'REVENUE', 'OTHERINCOME', 'OTHINCOME') THEN 'OTHER_INCOME'
        WHEN UPPER(ah.accttype) IN ('OTHERINCOME', 'OTHINCOME') THEN 'OTHER_INCOME'
        
        -- Other Expense: 8000-8999 series OR 9000+ with expense type
        WHEN ah.acctnumber BETWEEN '8000' AND '8999' THEN 'OTHER_EXPENSE'
        WHEN ah.acctnumber >= '9000' AND UPPER(ah.accttype) IN ('EXPENSE', 'OTHEREXPENSE', 'OTHEXPENSE') THEN 'OTHER_EXPENSE'
        WHEN ah.parent_account_number BETWEEN '8000' AND '8999' THEN 'OTHER_EXPENSE'
        WHEN UPPER(ah.accttype) IN ('OTHEREXPENSE', 'OTHEXPENSE') THEN 'OTHER_EXPENSE'
        
        -- Balance Sheet accounts
        WHEN UPPER(ah.accttype) IN ('BANK', 'ACCREC', 'INVENTORY', 'OTHCURRASSET', 'FIXEDASSET', 'ACCUMDEPRECIATION', 'OTHERASSET', 'DEFERREDEXPENSE', 'UNBILLEDRECEIVABLE') THEN 'ASSET'
        WHEN UPPER(ah.accttype) IN ('ACCTSPAY', 'CREDITCARD', 'OTHCURRLIAB', 'LONGTERMLIAB', 'DEFERREDREVENUE') THEN 'LIABILITY'
        WHEN UPPER(ah.accttype) IN ('EQUITY', 'RETEARNINGS', 'EQUITY-NOCLOSE', 'EQUITY-CLOSES') THEN 'EQUITY'
        
        ELSE 'OTHER'
      END as dynamic_transaction_type,
      
      -- DYNAMIC TRANSACTION CATEGORY CLASSIFICATION
      CASE 
        -- Revenue categories based on account number patterns and names
        WHEN ah.acctnumber BETWEEN '4100' AND '4109' OR UPPER(ah.fullname) LIKE '%RESERVE%' THEN 'RESERVE'
        WHEN ah.acctnumber BETWEEN '4110' AND '4119' OR UPPER(ah.fullname) LIKE '%VSC%' OR UPPER(ah.fullname) LIKE '%SERVICE CONTRACT%' THEN 'VSC'
        WHEN ah.acctnumber BETWEEN '4120' AND '4129' OR UPPER(ah.fullname) LIKE '%GAP%' OR UPPER(ah.fullname) LIKE '%GUARANTEED%' THEN 'GAP'
        WHEN ah.acctnumber BETWEEN '4130' AND '4139' OR UPPER(ah.fullname) LIKE '%DOC%' OR UPPER(ah.fullname) LIKE '%DOCUMENT%' THEN 'DOC_FEES'
        WHEN ah.acctnumber BETWEEN '4140' AND '4149' OR UPPER(ah.fullname) LIKE '%TITLING%' OR UPPER(ah.fullname) LIKE '%TITLE%' THEN 'TITLING_FEES'
        WHEN ah.acctnumber BETWEEN '4000' AND '4999' THEN 'GENERAL_REVENUE'
        
        -- Cost of Revenue categories
        WHEN ah.acctnumber BETWEEN '5300' AND '5399' OR UPPER(ah.fullname) LIKE '%PEOPLE%' OR UPPER(ah.fullname) LIKE '%EMPLOYEE%' OR UPPER(ah.fullname) LIKE '%PAYROLL%' THEN 'DIRECT_PEOPLE_COST'
        WHEN ah.acctnumber BETWEEN '5400' AND '5499' OR UPPER(ah.fullname) LIKE '%PAYOFF%' OR UPPER(ah.fullname) LIKE '%VARIANCE%' THEN 'PAYOFF_EXPENSE'
        WHEN ah.acctnumber BETWEEN '5500' AND '5599' OR (ah.acctnumber BETWEEN '5000' AND '5999' AND UPPER(ah.fullname) LIKE '%OTHER%') THEN 'OTHER_COR'
        WHEN ah.acctnumber BETWEEN '5000' AND '5999' THEN 'COST_OF_GOODS'
        
        -- Operating Expense categories
        WHEN ah.acctnumber BETWEEN '6000' AND '6499' OR UPPER(ah.fullname) LIKE '%SALARY%' OR UPPER(ah.fullname) LIKE '%WAGE%' OR UPPER(ah.fullname) LIKE '%COMMISSION%' THEN 'PEOPLE_COST'
        WHEN ah.acctnumber BETWEEN '6500' AND '6599' OR UPPER(ah.fullname) LIKE '%MARKETING%' OR UPPER(ah.fullname) LIKE '%ADVERTISING%' THEN 'MARKETING'
        WHEN ah.acctnumber BETWEEN '7000' AND '7999' OR UPPER(ah.fullname) LIKE '%GENERAL%' OR UPPER(ah.fullname) LIKE '%ADMINISTRATIVE%' THEN 'GA_EXPENSE'
        
        -- Tax and Other categories
        WHEN ah.acctnumber BETWEEN '8000' AND '8099' OR UPPER(ah.fullname) LIKE '%TAX%' THEN 'INCOME_TAX'
        WHEN ah.acctnumber BETWEEN '9000' AND '9099' AND UPPER(ah.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME') THEN 'INTEREST'
        WHEN ah.acctnumber BETWEEN '9000' AND '9099' THEN 'NON_OPERATING'
        
        -- Balance sheet categories
        WHEN UPPER(ah.accttype) = 'BANK' OR UPPER(ah.fullname) LIKE '%CASH%' THEN 'CASH'
        WHEN UPPER(ah.accttype) IN ('ACCREC', 'ACCOUNTSRECEIVABLE') THEN 'RECEIVABLES'
        WHEN UPPER(ah.accttype) = 'INVENTORY' THEN 'INVENTORY'
        WHEN UPPER(ah.accttype) IN ('OTHCURRASSET', 'OTHERCURRENTASSET') THEN 'CURRENT_ASSETS'
        WHEN UPPER(ah.accttype) IN ('FIXEDASSET', 'FIXEDASSETS') THEN 'FIXED_ASSETS'
        WHEN UPPER(ah.accttype) IN ('ACCTSPAY', 'ACCOUNTSPAYABLE') THEN 'PAYABLES'
        WHEN UPPER(ah.accttype) = 'CREDITCARD' THEN 'CREDIT_CARD'
        WHEN UPPER(ah.accttype) IN ('OTHCURRLIAB', 'OTHERCURRENTLIABILITY') THEN 'CURRENT_LIABILITIES'
        WHEN UPPER(ah.accttype) IN ('LONGTERMLIAB', 'LONGTERMLIABILITY') THEN 'LONG_TERM_DEBT'
        WHEN UPPER(ah.accttype) IN ('EQUITY', 'RETEARNINGS') THEN 'EQUITY'
        
        ELSE 'UNMAPPED'
      END as dynamic_transaction_category,
      
      -- DYNAMIC TRANSACTION SUBCATEGORY CLASSIFICATION
      CASE 
        WHEN UPPER(ah.fullname) LIKE '%BONUS%' OR UPPER(ah.acctnumber) LIKE '%B' THEN 'BONUS'
        WHEN UPPER(ah.fullname) LIKE '%ADVANCE%' OR UPPER(ah.acctnumber) LIKE '%A' THEN 'ADVANCE'
        WHEN UPPER(ah.fullname) LIKE '%CHARGEBACK%' OR UPPER(ah.acctnumber) LIKE '%C' THEN 'CHARGEBACK'
        WHEN UPPER(ah.fullname) LIKE '%VOLUME%' THEN 'VOLUME_BONUS'
        WHEN UPPER(ah.fullname) LIKE '%COST%' THEN 'COST'
        WHEN UPPER(ah.fullname) LIKE '%REINSURANCE%' THEN 'REINSURANCE'
        WHEN UPPER(ah.fullname) LIKE '%BASE%' OR ah.acctnumber NOT LIKE '%[A-Z]' THEN 'BASE'
        ELSE 'STANDARD'
      END as dynamic_transaction_subcategory
      
    FROM account_hierarchy ah
  )
  
  -- Select the latest distinct account data with DYNAMIC CLASSIFICATION
  SELECT  
    CAST(dc.id AS STRING) AS account_key,
    dc.id as account_id,
    dc.acctnumber AS account_number,
    COALESCE(dc.accountsearchdisplayname, dc.fullname, 'Unknown Account') AS account_name,
    dc.fullname AS account_full_name,
    dc.accttype AS account_type,
    
    -- Legacy categorization based on NetSuite account type mappings (kept for backward compatibility)
    CASE
      WHEN UPPER(dc.accttype) IN ('BANK', 'BANK-BANK') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('ACCREC', 'ACCTSRECEIVABLE', 'ACCOUNTSRECEIVABLE') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('INVENTORY') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('OTHCURRASSET', 'OTHERCURRENTASSET', 'OTHERCURRENTASSETS') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('FIXEDASSET', 'FIXEDASSETS') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('ACCUMDEPRECIATION', 'ACCUMDEPREC') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('OTHERASSET', 'OTHERASSETS') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('DEFERREDEXPENSE', 'DEFEREXPENSE') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('UNBILLEDRECEIVABLE', 'UNBILLEDREC') THEN 'Assets'
      WHEN UPPER(dc.accttype) IN ('ACCTSPAY', 'ACCOUNTSPAYABLE') THEN 'Liabilities'
      WHEN UPPER(dc.accttype) IN ('CREDITCARD') THEN 'Liabilities'
      WHEN UPPER(dc.accttype) IN ('OTHCURRLIAB', 'OTHERCURRENTLIABILITY', 'OTHERCURRENTLIABILITIES') THEN 'Liabilities'
      WHEN UPPER(dc.accttype) IN ('LONGTERMLIAB', 'LONGTERMLIABILITY', 'LONGTERMLABILITIES') THEN 'Liabilities'
      WHEN UPPER(dc.accttype) IN ('DEFERREDREVENUE', 'DEFERREVENUE') THEN 'Liabilities'
      WHEN UPPER(dc.accttype) IN ('EQUITY', 'EQUITY-NOCLOSE', 'EQUITYNOCLOSE') THEN 'Equity'
      WHEN UPPER(dc.accttype) IN ('RETEARNINGS', 'RETAINEDEARNINGS') THEN 'Equity'
      WHEN UPPER(dc.accttype) IN ('EQUITY-CLOSES', 'EQUITYCLOSES') THEN 'Equity'
      WHEN UPPER(dc.accttype) IN ('INCOME', 'REVENUE') THEN 'Revenue'
      WHEN UPPER(dc.accttype) IN ('OTHERINCOME', 'OTHINCOME') THEN 'Revenue'
      WHEN UPPER(dc.accttype) IN ('COGS', 'COSTOFGOODSSOLD') THEN 'Expenses'
      WHEN UPPER(dc.accttype) IN ('EXPENSE') THEN 'Expenses'
      WHEN UPPER(dc.accttype) IN ('OTHEREXPENSE', 'OTHEXPENSE') THEN 'Expenses'
      WHEN UPPER(dc.accttype) IN ('STATISTICAL') THEN 'Statistical'
      ELSE 'Other'
    END AS account_category,
    
    -- DYNAMIC transaction classification (replaces static mappings)
    dc.dynamic_transaction_type as transaction_type,
    dc.dynamic_transaction_category as transaction_category,
    dc.dynamic_transaction_subcategory as transaction_subcategory,
    
    -- DYNAMIC Income Statement Metric Groupings based on transaction classification
    CASE 
      WHEN dc.dynamic_transaction_type = 'REVENUE' THEN
        CASE 
          WHEN dc.dynamic_transaction_category = 'RESERVE' THEN 'REV_RESERVE'
          WHEN dc.dynamic_transaction_category = 'VSC' THEN 'REV_VSC'
          WHEN dc.dynamic_transaction_category = 'GAP' THEN 'REV_GAP'
          WHEN dc.dynamic_transaction_category = 'DOC_FEES' THEN 'REV_DOC_FEES'
          WHEN dc.dynamic_transaction_category = 'TITLING_FEES' THEN 'REV_TITLING_FEES'
          ELSE 'REV_OTHER'
        END
      ELSE NULL
    END as revenue_metric_group,
    
    CASE 
      WHEN dc.dynamic_transaction_type = 'COST_OF_REVENUE' THEN
        CASE 
          WHEN dc.dynamic_transaction_category = 'DIRECT_PEOPLE_COST' THEN 'COR_DIRECT_PEOPLE'
          WHEN dc.dynamic_transaction_category = 'PAYOFF_EXPENSE' THEN 'COR_PAYOFF_EXPENSE'
          WHEN dc.dynamic_transaction_category IN ('OTHER_COR', 'COST_OF_GOODS') THEN 'COR_OTHER'
          ELSE 'COR_OTHER'
        END
      ELSE NULL
    END as cost_metric_group,
    
    CASE 
      WHEN dc.dynamic_transaction_type = 'EXPENSE' THEN
        CASE 
          WHEN dc.dynamic_transaction_category = 'PEOPLE_COST' THEN 'PEOPLE_COST'
          WHEN dc.dynamic_transaction_category = 'MARKETING' THEN 'MARKETING'
          WHEN dc.dynamic_transaction_category = 'GA_EXPENSE' THEN 'GA_EXPENSE'
          ELSE 'OTHER_EXPENSE'
        END
      ELSE NULL
    END as expense_metric_group,
    
    CASE 
      WHEN dc.dynamic_transaction_type IN ('OTHER_INCOME', 'OTHER_EXPENSE') THEN
        CASE 
          WHEN dc.dynamic_transaction_type = 'OTHER_INCOME' THEN 'OTHER_INCOME'
          WHEN dc.dynamic_transaction_type = 'OTHER_EXPENSE' THEN 'OTHER_EXPENSE'
          ELSE NULL
        END
      ELSE NULL
    END as other_metric_group,
    
    -- DYNAMIC High-level P&L boolean flags (based on dynamic classification)
    (dc.dynamic_transaction_type = 'REVENUE') as is_total_revenue,
    (dc.dynamic_transaction_type = 'COST_OF_REVENUE') as is_cost_of_revenue,
    (dc.dynamic_transaction_type IN ('REVENUE', 'COST_OF_REVENUE')) as is_gross_profit,
    (dc.dynamic_transaction_type = 'EXPENSE') as is_operating_expense,
    (dc.dynamic_transaction_type IN ('REVENUE', 'COST_OF_REVENUE', 'EXPENSE')) as is_net_ordinary_revenue,
    (dc.dynamic_transaction_type IN ('OTHER_INCOME', 'OTHER_EXPENSE')) as is_other_income_expense,
    (dc.dynamic_transaction_type IN ('REVENUE', 'COST_OF_REVENUE', 'EXPENSE', 'OTHER_INCOME', 'OTHER_EXPENSE')) as is_net_income,
    
    CASE 
      WHEN dc.parent IS NOT NULL AND dc.parent != 0 THEN CAST(dc.parent AS STRING) 
      ELSE NULL 
    END AS parent_account_key,
    COALESCE(dc.issummary = 'T', FALSE) AS is_summary,
    COALESCE(dc.isinactive = 'T', FALSE) AS is_inactive,
    COALESCE(dc.inventory = 'T', FALSE) AS is_inventory,
    dc.description AS description,
    dc.subsidiary AS subsidiary,
    dc.includechildren AS include_children,
    dc.eliminate,
    dc.revalue,
    dc.reconcilewithmatching AS reconcile_with_matching,
    dc.sbankname AS bank_name,
    dc.sbankroutingnumber AS bank_routing_number,
    dc.sspecacct AS special_account,
    dc.externalid AS external_id,
    'bronze.ns.account' AS _source_table
  FROM dynamic_classification dc
  WHERE dc.id IS NOT NULL 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dc.id ORDER BY dc.lastmodifieddate DESC NULLS LAST) = 1

) AS source
ON target.account_key = source.account_key

WHEN MATCHED THEN
  UPDATE SET
    target.account_id = source.account_id,
    target.account_number = source.account_number,
    target.account_name = source.account_name,
    target.account_full_name = source.account_full_name,
    target.account_type = source.account_type,
    target.account_category = source.account_category,
    target.transaction_type = source.transaction_type,
    target.transaction_category = source.transaction_category,
    target.transaction_subcategory = source.transaction_subcategory,
    target.revenue_metric_group = source.revenue_metric_group,
    target.cost_metric_group = source.cost_metric_group,
    target.expense_metric_group = source.expense_metric_group,
    target.other_metric_group = source.other_metric_group,
    target.is_total_revenue = source.is_total_revenue,
    target.is_cost_of_revenue = source.is_cost_of_revenue,
    target.is_gross_profit = source.is_gross_profit,
    target.is_operating_expense = source.is_operating_expense,
    target.is_net_ordinary_revenue = source.is_net_ordinary_revenue,
    target.is_other_income_expense = source.is_other_income_expense,
    target.is_net_income = source.is_net_income,
    target.parent_account_key = source.parent_account_key,
    target.is_summary = source.is_summary,
    target.is_inactive = source.is_inactive,
    target.is_inventory = source.is_inventory,
    target.description = source.description,
    target.subsidiary = source.subsidiary,
    target.include_children = source.include_children,
    target.eliminate = source.eliminate,
    target.revalue = source.revalue,
    target.reconcile_with_matching = source.reconcile_with_matching,
    target.bank_name = source.bank_name,
    target.bank_routing_number = source.bank_routing_number,
    target.special_account = source.special_account,
    target.external_id = source.external_id,
    target._load_timestamp = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    account_key, account_id, account_number, account_name, account_full_name,
    account_type, account_category, transaction_type, transaction_category, transaction_subcategory,
    revenue_metric_group, cost_metric_group, expense_metric_group, other_metric_group,
    is_total_revenue, is_cost_of_revenue, is_gross_profit, is_operating_expense,
    is_net_ordinary_revenue, is_other_income_expense, is_net_income,
    parent_account_key, is_summary, is_inactive, is_inventory, description,
    subsidiary, include_children, eliminate, revalue, reconcile_with_matching,
    bank_name, bank_routing_number, special_account, external_id, _source_table, _load_timestamp
  )
  VALUES (
    source.account_key, source.account_id, source.account_number, source.account_name, source.account_full_name,
    source.account_type, source.account_category, source.transaction_type, source.transaction_category, source.transaction_subcategory,
    source.revenue_metric_group, source.cost_metric_group, source.expense_metric_group, source.other_metric_group,
    source.is_total_revenue, source.is_cost_of_revenue, source.is_gross_profit, source.is_operating_expense,
    source.is_net_ordinary_revenue, source.is_other_income_expense, source.is_net_income,
    source.parent_account_key, source.is_summary, source.is_inactive, source.is_inventory, source.description,
    source.subsidiary, source.include_children, source.eliminate, source.revalue, source.reconcile_with_matching,
    source.bank_name, source.bank_routing_number, source.special_account, source.external_id, source._source_table, CURRENT_TIMESTAMP()
  );

-- Ensure 'No Account' exists for handling NULLs
MERGE INTO silver.finance.dim_account AS target
USING (
  SELECT '0' as account_key, 0 as account_id, NULL as account_number, 'No Account' as account_name, 
         'No Account' as account_full_name, 'Other' as account_type, 'Other' as account_category, 
         'OTHER' as transaction_type, 'UNMAPPED' as transaction_category, 'STANDARD' as transaction_subcategory, 
         NULL as revenue_metric_group, NULL as cost_metric_group, NULL as expense_metric_group, NULL as other_metric_group, 
         false as is_total_revenue, false as is_cost_of_revenue, false as is_gross_profit, false as is_operating_expense, 
         false as is_net_ordinary_revenue, false as is_other_income_expense, false as is_net_income, 
         NULL as parent_account_key, false as is_summary, false as is_inactive, false as is_inventory, 
         'Default account for null values' as description, NULL as subsidiary, NULL as include_children, 
         NULL as eliminate, NULL as revalue, NULL as reconcile_with_matching, NULL as bank_name, 
         NULL as bank_routing_number, NULL as special_account, NULL as external_id, 'static' as _source_table
) AS source
ON target.account_key = source.account_key
WHEN NOT MATCHED THEN 
  INSERT (
    account_key, account_id, account_number, account_name, account_full_name,
    account_type, account_category, transaction_type, transaction_category, transaction_subcategory,
    revenue_metric_group, cost_metric_group, expense_metric_group, other_metric_group,
    is_total_revenue, is_cost_of_revenue, is_gross_profit, is_operating_expense,
    is_net_ordinary_revenue, is_other_income_expense, is_net_income,
    parent_account_key, is_summary, is_inactive, is_inventory, description,
    subsidiary, include_children, eliminate, revalue, reconcile_with_matching,
    bank_name, bank_routing_number, special_account, external_id, _source_table, _load_timestamp
  )
  VALUES (
    source.account_key, source.account_id, source.account_number, source.account_name, source.account_full_name,
    source.account_type, source.account_category, source.transaction_type, source.transaction_category, source.transaction_subcategory,
    source.revenue_metric_group, source.cost_metric_group, source.expense_metric_group, source.other_metric_group,
    source.is_total_revenue, source.is_cost_of_revenue, source.is_gross_profit, source.is_operating_expense,
    source.is_net_ordinary_revenue, source.is_other_income_expense, source.is_net_income,
    source.parent_account_key, source.is_summary, source.is_inactive, source.is_inventory, source.description,
    source.subsidiary, source.include_children, source.eliminate, source.revalue, source.reconcile_with_matching,
    source.bank_name, source.bank_routing_number, source.special_account, source.external_id, source._source_table, CURRENT_TIMESTAMP()
  );
