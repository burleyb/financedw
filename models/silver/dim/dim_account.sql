-- models/silver/dim/dim_account.sql
-- Enhanced Account Dimension with Income Statement Groupings

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
COMMENT 'Silver layer NetSuite accounts dimension'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO silver.finance.dim_account AS target
USING (
  WITH business_account_overrides AS (
    -- Business-specific account mappings (same as fact table)
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
      ('5510', 'COST_OF_REVENUE', 'OTHER_COR', 'POSTAGE'),

      -- GA_EXPENSE overrides
      ('7175', 'EXPENSE', 'GA_EXPENSE', 'DEPRECIATION'),
      ('7176', 'EXPENSE', 'GA_EXPENSE', 'AMORTIZATION')
    AS t(account_number, transaction_type, transaction_category, transaction_subcategory)
  ),
  
  metric_group_mappings AS (
    -- Define income statement metric groupings
    SELECT account_number, metric_type, metric_group FROM VALUES
      -- Revenue Metric Groups
      ('4105', 'REVENUE', 'REV_RESERVE'),
      ('4106', 'REVENUE', 'REV_RESERVE'),
      ('4107', 'REVENUE', 'REV_RESERVE'),
      ('4110', 'REVENUE', 'REV_VSC'),
      ('4110A', 'REVENUE', 'REV_VSC'),
      ('4110B', 'REVENUE', 'REV_VSC'),
      ('4110C', 'REVENUE', 'REV_VSC'), -- VSC Cost treated as negative revenue
      ('4111', 'REVENUE', 'REV_VSC'),
      ('4115', 'REVENUE', 'REV_VSC'),
      ('4120', 'REVENUE', 'REV_GAP'),
      ('4120A', 'REVENUE', 'REV_GAP'),
      ('4120B', 'REVENUE', 'REV_GAP'),
      ('4120C', 'REVENUE', 'REV_GAP'), -- GAP Cost treated as negative revenue
      ('4121', 'REVENUE', 'REV_GAP'),
      ('4125', 'REVENUE', 'REV_GAP'),
      ('4130', 'REVENUE', 'REV_DOC_FEES'),
      ('4130C', 'REVENUE', 'REV_DOC_FEES'),
      ('4141', 'REVENUE', 'REV_TITLING_FEES'),
      ('4142', 'REVENUE', 'REV_TITLING_FEES'),
      ('4190', 'REVENUE', 'REV_OTHER'),
      
      -- Cost of Revenue Metric Groups
      ('5301', 'COST_OF_REVENUE', 'COR_DIRECT_PEOPLE'),
      ('5304', 'COST_OF_REVENUE', 'COR_DIRECT_PEOPLE'),
      ('5305', 'COST_OF_REVENUE', 'COR_DIRECT_PEOPLE'),
      ('5320', 'COST_OF_REVENUE', 'COR_DIRECT_PEOPLE'),
      ('5330', 'COST_OF_REVENUE', 'COR_DIRECT_PEOPLE'),
      ('5340', 'COST_OF_REVENUE', 'COR_DIRECT_PEOPLE'),
      ('5400', 'COST_OF_REVENUE', 'COR_PAYOFF_EXPENSE'),
      ('5401', 'COST_OF_REVENUE', 'COR_PAYOFF_EXPENSE'),
      ('5402', 'COST_OF_REVENUE', 'COR_REGISTRATION'),
      ('5403', 'COST_OF_REVENUE', 'COR_PAYOFF_EXPENSE'),
      ('5404', 'COST_OF_REVENUE', 'COR_PAYOFF_EXPENSE'),
      ('5520', 'COST_OF_REVENUE', 'COR_PAYOFF_EXPENSE'),
      ('5141', 'COST_OF_REVENUE', 'COR_REGISTRATION'),
      ('5530', 'COST_OF_REVENUE', 'COR_REGISTRATION'),
      ('5199', 'COST_OF_REVENUE', 'COR_OTHER'),
      ('5510', 'COST_OF_REVENUE', 'COR_OTHER'),
      
      -- Operating Expense Metric Groups
      ('6000', 'EXPENSE', 'PEOPLE_COST'),
      ('6010', 'EXPENSE', 'PEOPLE_COST'),
      ('6011', 'EXPENSE', 'PEOPLE_COST'),
      ('6013', 'EXPENSE', 'PEOPLE_COST'),
      ('6030', 'EXPENSE', 'PEOPLE_COST'),
      ('6040', 'EXPENSE', 'PEOPLE_COST'),
      ('6041', 'EXPENSE', 'PEOPLE_COST'),
      ('6100', 'EXPENSE', 'PEOPLE_COST'),
      ('6101', 'EXPENSE', 'PEOPLE_COST'),
      ('6110', 'EXPENSE', 'PEOPLE_COST'),
      ('6200', 'EXPENSE', 'PEOPLE_COST'),
      ('6510', 'EXPENSE', 'MARKETING'),
      ('6520', 'EXPENSE', 'MARKETING'),
      ('6530', 'EXPENSE', 'MARKETING'),
      ('6540', 'EXPENSE', 'MARKETING'),
      ('6550', 'EXPENSE', 'MARKETING'),
      ('6560', 'EXPENSE', 'MARKETING'),
      ('6570', 'EXPENSE', 'MARKETING'),
      ('7000', 'EXPENSE', 'GA_EXPENSE'),
      ('7001', 'EXPENSE', 'GA_EXPENSE'),
      ('7002', 'EXPENSE', 'GA_EXPENSE'),
      ('7003', 'EXPENSE', 'GA_EXPENSE'),
      ('7004', 'EXPENSE', 'GA_EXPENSE'),
      ('7005', 'EXPENSE', 'GA_EXPENSE'),
      ('7006', 'EXPENSE', 'GA_EXPENSE'),
      ('7007', 'EXPENSE', 'GA_EXPENSE'),
      ('7100', 'EXPENSE', 'GA_EXPENSE'),
      ('7110', 'EXPENSE', 'GA_EXPENSE'),
      ('7120', 'EXPENSE', 'GA_EXPENSE'),
      ('7125', 'EXPENSE', 'GA_EXPENSE'),
      ('7130', 'EXPENSE', 'GA_EXPENSE'),
      ('7131', 'EXPENSE', 'GA_EXPENSE'),
      ('7140', 'EXPENSE', 'GA_EXPENSE'),
      ('7141', 'EXPENSE', 'GA_EXPENSE'),
      ('7150', 'EXPENSE', 'GA_EXPENSE'),
      ('7160', 'EXPENSE', 'GA_EXPENSE'),
      ('7170', 'EXPENSE', 'GA_EXPENSE'),
      ('7170A', 'EXPENSE', 'GA_EXPENSE'),
      ('7171', 'EXPENSE', 'GA_EXPENSE'),
      ('7172', 'EXPENSE', 'GA_EXPENSE'),
      ('7174', 'EXPENSE', 'GA_EXPENSE'),
      ('7175', 'EXPENSE', 'GA_EXPENSE'),
      ('7176', 'EXPENSE', 'GA_EXPENSE'),
      ('7180', 'EXPENSE', 'GA_EXPENSE'),
      ('7190', 'EXPENSE', 'GA_EXPENSE'),
      ('7192', 'EXPENSE', 'GA_EXPENSE'),
      ('8000', 'EXPENSE', 'GA_EXPENSE'),
      
      -- Other Income/Expense Metric Groups
      ('8010', 'OTHER_EXPENSE', 'OTHER_EXPENSE'),
      ('8011', 'OTHER_EXPENSE', 'OTHER_EXPENSE'),
      ('9001', 'OTHER_INCOME', 'OTHER_INCOME'),
      ('9001D', 'OTHER_INCOME', 'OTHER_INCOME'),
      ('9007', 'OTHER_EXPENSE', 'OTHER_EXPENSE'),
      ('9010', 'OTHER_EXPENSE', 'OTHER_EXPENSE')
    AS t(account_number, metric_type, metric_group)
  )
  
  -- Select the latest distinct account data from the bronze NetSuite accounts table
  SELECT  
    CAST(a.id AS STRING) AS account_key, -- Natural Key
    a.id as account_id,
    a.acctnumber AS account_number,
    COALESCE(a.accountsearchdisplayname, a.fullname, 'Unknown Account') AS account_name,
    a.fullname AS account_full_name,
    a.accttype AS account_type,
    
    -- Legacy categorization based on NetSuite account type mappings (kept for backward compatibility)
    CASE
      -- Asset accounts (Balance Sheet)
      WHEN UPPER(a.accttype) IN ('BANK', 'BANK-BANK') THEN 'Assets'
      WHEN UPPER(a.accttype) IN ('ACCREC', 'ACCTSRECEIVABLE', 'ACCOUNTSRECEIVABLE') THEN 'Assets'
      WHEN UPPER(a.accttype) IN ('INVENTORY') THEN 'Assets'
      WHEN UPPER(a.accttype) IN ('OTHCURRASSET', 'OTHERCURRENTASSET', 'OTHERCURRENTASSETS') THEN 'Assets'
      WHEN UPPER(a.accttype) IN ('FIXEDASSET', 'FIXEDASSETS') THEN 'Assets'
      WHEN UPPER(a.accttype) IN ('ACCUMDEPRECIATION', 'ACCUMDEPREC') THEN 'Assets'
      WHEN UPPER(a.accttype) IN ('OTHERASSET', 'OTHERASSETS') THEN 'Assets'
      WHEN UPPER(a.accttype) IN ('DEFERREDEXPENSE', 'DEFEREXPENSE') THEN 'Assets'
      WHEN UPPER(a.accttype) IN ('UNBILLEDRECEIVABLE', 'UNBILLEDREC') THEN 'Assets'
      
      -- Liability accounts (Balance Sheet)
      WHEN UPPER(a.accttype) IN ('ACCTSPAY', 'ACCOUNTSPAYABLE') THEN 'Liabilities'
      WHEN UPPER(a.accttype) IN ('CREDITCARD') THEN 'Liabilities'
      WHEN UPPER(a.accttype) IN ('OTHCURRLIAB', 'OTHERCURRENTLIABILITY', 'OTHERCURRENTLIABILITIES') THEN 'Liabilities'
      WHEN UPPER(a.accttype) IN ('LONGTERMLIAB', 'LONGTERMLIABILITY', 'LONGTERMLABILITIES') THEN 'Liabilities'
      WHEN UPPER(a.accttype) IN ('DEFERREDREVENUE', 'DEFERREVENUE') THEN 'Liabilities'
      
      -- Equity accounts (Balance Sheet)
      WHEN UPPER(a.accttype) IN ('EQUITY', 'EQUITY-NOCLOSE', 'EQUITYNOCLOSE') THEN 'Equity'
      WHEN UPPER(a.accttype) IN ('RETEARNINGS', 'RETAINEDEARNINGS') THEN 'Equity'
      WHEN UPPER(a.accttype) IN ('EQUITY-CLOSES', 'EQUITYCLOSES') THEN 'Equity'
      
      -- Income/Revenue accounts (Income Statement)
      WHEN UPPER(a.accttype) IN ('INCOME', 'REVENUE') THEN 'Revenue'
      WHEN UPPER(a.accttype) IN ('OTHERINCOME', 'OTHINCOME') THEN 'Revenue'
      
      -- Expense accounts (Income Statement)
      WHEN UPPER(a.accttype) IN ('COGS', 'COSTOFGOODSSOLD') THEN 'Expenses'
      WHEN UPPER(a.accttype) IN ('EXPENSE') THEN 'Expenses'
      WHEN UPPER(a.accttype) IN ('OTHEREXPENSE', 'OTHEXPENSE') THEN 'Expenses'
      
      -- Special account types
      WHEN UPPER(a.accttype) IN ('STATISTICAL') THEN 'Statistical'
      
      ELSE 'Other'
    END AS account_category,
    
    -- Enhanced transaction classification
    COALESCE(
      bao.transaction_type,
      CASE 
        WHEN a.acctnumber BETWEEN '4000' AND '4999' THEN 'REVENUE'
        WHEN a.acctnumber BETWEEN '5000' AND '5999' THEN 'COST_OF_REVENUE'
        WHEN a.acctnumber BETWEEN '6000' AND '7999' THEN 'EXPENSE'
        WHEN a.acctnumber BETWEEN '8000' AND '8999' THEN 
          CASE 
            WHEN UPPER(a.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME') THEN 'OTHER_INCOME'
            ELSE 'OTHER_EXPENSE'
          END
        WHEN UPPER(a.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME', 'OTHINCOME') THEN 'REVENUE'
        WHEN UPPER(a.accttype) IN ('COGS', 'COSTOFGOODSSOLD') THEN 'COST_OF_REVENUE'
        WHEN UPPER(a.accttype) IN ('EXPENSE', 'OTHEREXPENSE', 'OTHEXPENSE') THEN 'EXPENSE'
        WHEN UPPER(a.accttype) IN ('BANK', 'ACCREC', 'INVENTORY', 'OTHCURRASSET', 'FIXEDASSET', 'ACCUMDEPRECIATION', 'OTHERASSET') THEN 'ASSET'
        WHEN UPPER(a.accttype) IN ('ACCTSPAY', 'CREDITCARD', 'OTHCURRLIAB', 'LONGTERMLIAB') THEN 'LIABILITY'
        WHEN UPPER(a.accttype) IN ('EQUITY', 'RETEARNINGS') THEN 'EQUITY'
        ELSE 'OTHER'
      END
    ) as transaction_type,
    
    COALESCE(
      bao.transaction_category,
      CASE 
        WHEN a.acctnumber BETWEEN '4100' AND '4109' THEN 'RESERVE'
        WHEN a.acctnumber BETWEEN '4110' AND '4119' THEN 'VSC'
        WHEN a.acctnumber BETWEEN '4120' AND '4129' THEN 'GAP'
        WHEN a.acctnumber BETWEEN '4130' AND '4139' THEN 'DOC_FEES'
        WHEN a.acctnumber BETWEEN '4140' AND '4149' THEN 'TITLING_FEES'
        WHEN a.acctnumber BETWEEN '4000' AND '4999' THEN 'GENERAL_REVENUE'
        WHEN a.acctnumber BETWEEN '5300' AND '5399' THEN 'DIRECT_PEOPLE_COST'
        WHEN a.acctnumber BETWEEN '5400' AND '5499' THEN 'PAYOFF_EXPENSE'
        WHEN a.acctnumber BETWEEN '5500' AND '5599' THEN 'OTHER_COR'
        WHEN a.acctnumber BETWEEN '5000' AND '5999' THEN 'COST_OF_GOODS'
        WHEN a.acctnumber BETWEEN '6000' AND '6499' THEN 'PEOPLE_COST'
        WHEN a.acctnumber BETWEEN '6500' AND '6599' THEN 'MARKETING'
        WHEN a.acctnumber BETWEEN '7000' AND '7999' THEN 'GA_EXPENSE'
        WHEN a.acctnumber BETWEEN '8000' AND '8099' THEN 'INCOME_TAX'
        WHEN a.acctnumber BETWEEN '9000' AND '9099' AND UPPER(a.accttype) IN ('INCOME', 'REVENUE', 'OTHERINCOME') THEN 'INTEREST'
        WHEN a.acctnumber BETWEEN '9000' AND '9099' THEN 'NON_OPERATING'
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
    
    COALESCE(
      bao.transaction_subcategory,
      'STANDARD'
    ) as transaction_subcategory,
    
    -- Income Statement Metric Groupings
    CASE 
      WHEN mgm_rev.metric_group IS NOT NULL THEN mgm_rev.metric_group
      ELSE NULL
    END as revenue_metric_group,
    
    CASE 
      WHEN mgm_cor.metric_group IS NOT NULL THEN mgm_cor.metric_group
      ELSE NULL
    END as cost_metric_group,
    
    CASE 
      WHEN mgm_exp.metric_group IS NOT NULL THEN mgm_exp.metric_group
      ELSE NULL
    END as expense_metric_group,
    
    CASE 
      WHEN mgm_other.metric_group IS NOT NULL THEN mgm_other.metric_group
      ELSE NULL
    END as other_metric_group,
    
    
    -- High-level P&L boolean flags for easy filtering
    (a.acctnumber IN ('4105','4106','4107','4110','4110A','4110B','4110C','4111','4115','4120','4120A','4120B','4120C','4121','4125','4130','4130C','4141','4142','4190')) as is_total_revenue,
    (a.acctnumber IN ('5301','5304','5305','5320','5330','5340','5400','5401','5520','5402','5141','5530','5403','5404','5199','5510')) as is_cost_of_revenue,
    (a.acctnumber IN ('4105','4106','4107','4110','4110A','4110B','4110C','4111','4115','4120','4120A','4120B','4120C','4121','4125','4130','4130C','4141','4142','4190','5301','5305','5304','5320','5330','5340','5400','5520','5401','5402','5141','5530','5403','5404','5199','5510')) as is_gross_profit,
    (a.acctnumber IN ('6000','6010','6011','6013','6030','6040','6041','6100','6101','6110','6200','6510','6520','6530','6540','6550','6560','6570','7005','7006','7100','7110','7120','7125','7130','7131','7140','7141','7150','7160','7170','7171','7172','7174','7175','7176','7180','7190','8000')) as is_operating_expense,
    (a.acctnumber IN ('4105','4106','4107','4110','4110A','4110B','4110C','4111','4115','4120','4120A','4120B','4120C','4121','4125','4130','4130C','4141','4142','4190','5301','5305','5304','5320','5330','5340','5400','5520','5401','5402','5141','5530','5403','5404','5199','5510','6000','6010','6011','6013','6030','6040','6041','6100','6101','6110','6200','6510','6520','6530','6540','6550','6560','6570','7005','7006','7100','7110','7120','7125','7130','7131','7140','7141','7150','7160','7170','7171','7172','7174','7175','7176','7180','7190','8000','9001','9001D','8010','8011','9007')) as is_net_ordinary_revenue,
    (a.acctnumber IN ('9001','9001D','8010','8011','9007','9010')) as is_other_income_expense,
    (a.acctnumber IN ('4105','4106','4107','4110','4110A','4110B','4110C','4111','4115','4120','4120A','4120B','4120C','4121','4125','4130','4130C','4141','4142','4190','5301','5305','5304','5320','5330','5340','5400','5520','5401','5402','5141','5530','5403','5404','5199','5510','6000','6010','6011','6013','6030','6040','6041','6100','6101','6110','6200','6510','6520','6530','6540','6550','6560','6570','7005','7006','7100','7110','7120','7125','7130','7131','7140','7141','7150','7160','7170','7171','7172','7174','7175','7176','7180','7190','8000','9001','9001D','8010','8011','9007','9010')) as is_net_income,
    
    CASE 
      WHEN a.parent IS NOT NULL AND a.parent != 0 THEN CAST(a.parent AS STRING) 
      ELSE NULL 
    END AS parent_account_key, -- Fixed: Handle empty/null parent values safely for BIGINT source field
    COALESCE(a.issummary = 'T', FALSE) AS is_summary,
    COALESCE(a.isinactive = 'T', FALSE) AS is_inactive,
    COALESCE(a.inventory = 'T', FALSE) AS is_inventory,
    a.description AS description,
    a.subsidiary AS subsidiary,
    a.includechildren AS include_children,
    a.eliminate,
    a.revalue,
    a.reconcilewithmatching AS reconcile_with_matching,
    a.sbankname AS bank_name,
    a.sbankroutingnumber AS bank_routing_number,
    a.sspecacct AS special_account,
    a.externalid AS external_id,
    'bronze.ns.account' AS _source_table
  FROM bronze.ns.account a
  LEFT JOIN business_account_overrides bao ON a.acctnumber = bao.account_number
  LEFT JOIN metric_group_mappings mgm_rev ON a.acctnumber = mgm_rev.account_number AND mgm_rev.metric_type = 'REVENUE'
  LEFT JOIN metric_group_mappings mgm_cor ON a.acctnumber = mgm_cor.account_number AND mgm_cor.metric_type = 'COST_OF_REVENUE'
  LEFT JOIN metric_group_mappings mgm_exp ON a.acctnumber = mgm_exp.account_number AND mgm_exp.metric_type = 'EXPENSE'
  LEFT JOIN metric_group_mappings mgm_other ON a.acctnumber = mgm_other.account_number AND mgm_other.metric_type IN ('OTHER_INCOME', 'OTHER_EXPENSE')
  WHERE a.id IS NOT NULL 
    AND a._fivetran_deleted = FALSE -- Exclude deleted records
  -- Deduplicate based on ID, taking the most recently updated record
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.lastmodifieddate DESC NULLS LAST) = 1
) AS source
ON target.account_key = source.account_key

-- Update existing accounts if their data has changed (SCD Type 1)
WHEN MATCHED AND (
    target.account_id <> source.account_id OR
    target.account_number <> source.account_number OR
    target.account_name <> source.account_name OR
    target.account_full_name <> source.account_full_name OR
    target.account_type <> source.account_type OR
    target.account_category <> source.account_category OR
    target.transaction_type <> source.transaction_type OR
    target.transaction_category <> source.transaction_category OR
    target.transaction_subcategory <> source.transaction_subcategory OR
    target.revenue_metric_group <> source.revenue_metric_group OR
    target.cost_metric_group <> source.cost_metric_group OR
    target.expense_metric_group <> source.expense_metric_group OR
    target.other_metric_group <> source.other_metric_group OR
    target.is_total_revenue <> source.is_total_revenue OR
    target.is_cost_of_revenue <> source.is_cost_of_revenue OR
    target.is_gross_profit <> source.is_gross_profit OR
    target.is_operating_expense <> source.is_operating_expense OR
    target.is_net_ordinary_revenue <> source.is_net_ordinary_revenue OR
    target.is_other_income_expense <> source.is_other_income_expense OR
    target.is_net_income <> source.is_net_income OR
    target.parent_account_key <> source.parent_account_key OR
    target.is_summary <> source.is_summary OR
    target.is_inactive <> source.is_inactive OR
    target.is_inventory <> source.is_inventory OR
    target.description <> source.description OR
    target.subsidiary <> source.subsidiary OR
    target.include_children <> source.include_children OR
    target.eliminate <> source.eliminate OR
    target.revalue <> source.revalue OR
    target.reconcile_with_matching <> source.reconcile_with_matching OR
    target.bank_name <> source.bank_name OR
    target.bank_routing_number <> source.bank_routing_number OR
    target.special_account <> source.special_account OR
    target.external_id <> source.external_id
) THEN
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

-- Insert new accounts
WHEN NOT MATCHED THEN
  INSERT (
    account_key,
    account_id,
    account_number,
    account_name,
    account_full_name,
    account_type,
    account_category,
    transaction_type,
    transaction_category,
    transaction_subcategory,
    revenue_metric_group,
    cost_metric_group,
    expense_metric_group,
    other_metric_group,
    is_total_revenue,
    is_cost_of_revenue,
    is_gross_profit,
    is_operating_expense,
    is_net_ordinary_revenue,
    is_other_income_expense,
    is_net_income,
    parent_account_key,
    is_summary,
    is_inactive,
    is_inventory,
    description,
    subsidiary,
    include_children,
    eliminate,
    revalue,
    reconcile_with_matching,
    bank_name,
    bank_routing_number,
    special_account,
    external_id,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.account_key,
    source.account_id,
    source.account_number,
    source.account_name,
    source.account_full_name,
    source.account_type,
    source.account_category,
    source.transaction_type,
    source.transaction_category,
    source.transaction_subcategory,
    source.revenue_metric_group,
    source.cost_metric_group,
    source.expense_metric_group,
    source.other_metric_group,
    source.is_total_revenue,
    source.is_cost_of_revenue,
    source.is_gross_profit,
    source.is_operating_expense,
    source.is_net_ordinary_revenue,
    source.is_other_income_expense,
    source.is_net_income,
    source.parent_account_key,
    source.is_summary,
    source.is_inactive,
    source.is_inventory,
    source.description,
    source.subsidiary,
    source.include_children,
    source.eliminate,
    source.revalue,
    source.reconcile_with_matching,
    source.bank_name,
    source.bank_routing_number,
    source.special_account,
    source.external_id,
    source._source_table,
    CURRENT_TIMESTAMP()
  );

-- Ensure 'No Account' and 'Unknown' types exist for handling NULLs
MERGE INTO silver.finance.dim_account AS target
USING (
  SELECT '0' as account_key, 0 as account_id, NULL as account_number, 'No Account' as account_name, 'No Account' as account_full_name, 'Other' as account_type, 'Other' as account_category, 'OTHER' as transaction_type, 'UNMAPPED' as transaction_category, 'STANDARD' as transaction_subcategory, NULL as revenue_metric_group, NULL as cost_metric_group, NULL as expense_metric_group, NULL as other_metric_group, false as is_total_revenue, false as is_cost_of_revenue, false as is_gross_profit, false as is_operating_expense, false as is_net_ordinary_revenue, false as is_other_income_expense, false as is_net_income, NULL as parent_account_key, false as is_summary, false as is_inactive, false as is_inventory, 'Default account for null values' as description, NULL as subsidiary, NULL as include_children, NULL as eliminate, NULL as revalue, NULL as reconcile_with_matching, NULL as bank_name, NULL as bank_routing_number, NULL as special_account, NULL as external_id, 'static' as _source_table
) AS source
ON target.account_key = source.account_key
WHEN NOT MATCHED THEN 
  INSERT (
    account_key, 
    account_id,
    account_number,
    account_name, 
    account_full_name,
    account_type,
    account_category,
    transaction_type,
    transaction_category,
    transaction_subcategory,
    revenue_metric_group,
    cost_metric_group,
    expense_metric_group,
    other_metric_group,
    is_total_revenue,
    is_cost_of_revenue,
    is_gross_profit,
    is_operating_expense,
    is_net_ordinary_revenue,
    is_other_income_expense,
    is_net_income,
    parent_account_key,
    is_summary,
    is_inactive,
    is_inventory,
    description,
    subsidiary,
    include_children,
    eliminate,
    revalue,
    reconcile_with_matching,
    bank_name,
    bank_routing_number,
    special_account,
    external_id,
    _source_table, 
    _load_timestamp
  )
  VALUES (
    source.account_key, 
    source.account_id,
    source.account_number,
    source.account_name, 
    source.account_full_name,
    source.account_type,
    source.account_category,
    source.transaction_type,
    source.transaction_category,
    source.transaction_subcategory,
    source.revenue_metric_group,
    source.cost_metric_group,
    source.expense_metric_group,
    source.other_metric_group,
    source.is_total_revenue,
    source.is_cost_of_revenue,
    source.is_gross_profit,
    source.is_operating_expense,
    source.is_net_ordinary_revenue,
    source.is_other_income_expense,
    source.is_net_income,
    source.parent_account_key,
    source.is_summary,
    source.is_inactive,
    source.is_inventory,
    source.description,
    source.subsidiary,
    source.include_children,
    source.eliminate,
    source.revalue,
    source.reconcile_with_matching,
    source.bank_name,
    source.bank_routing_number,
    source.special_account,
    source.external_id,
    source._source_table, 
    CURRENT_TIMESTAMP()
  ); 