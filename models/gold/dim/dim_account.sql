-- models/gold/dim/dim_account.sql
-- Gold layer NetSuite accounts dimension with business enhancements
DROP TABLE IF EXISTS gold.finance.dim_account;

CREATE TABLE IF NOT EXISTS gold.finance.dim_account (
  account_key STRING NOT NULL,
  account_id BIGINT,
  account_number STRING,
  account_name STRING,
  account_full_name STRING,
  account_type STRING,
  account_category STRING,
  account_subcategory STRING,
  
  -- Income statement classification fields from silver layer
  transaction_type STRING, -- REVENUE, COST_OF_REVENUE, EXPENSE, OTHER_INCOME, OTHER_EXPENSE, ASSET, LIABILITY, EQUITY
  transaction_category STRING, -- RESERVE, VSC, GAP, DOC_FEES, DIRECT_PEOPLE_COST, etc.
  transaction_subcategory STRING, -- BASE, ADVANCE, BONUS, CHARGEBACK, etc.
  
  -- Income Statement Metric Groupings (from silver layer)
  revenue_metric_group STRING, -- REV_RESERVE, REV_VSC, REV_GAP, REV_DOC_FEES, etc.
  cost_metric_group STRING, -- COR_DIRECT_PEOPLE, COR_PAYOFF_EXPENSE, COR_OTHER, etc.
  expense_metric_group STRING, -- PEOPLE_COST, MARKETING, GA_EXPENSE, etc.
  other_metric_group STRING, -- OTHER_INCOME, OTHER_EXPENSE, etc.
  
  -- High-level P&L Groupings (from silver layer)
  is_total_revenue BOOLEAN, -- Part of total revenue calculation
  is_cost_of_revenue BOOLEAN, -- Part of cost of revenue calculation
  is_gross_profit BOOLEAN, -- Part of gross profit calculation (revenue - COR)
  is_operating_expense BOOLEAN, -- Part of operating expense calculation
  is_net_ordinary_revenue BOOLEAN, -- Part of net ordinary revenue calculation
  is_other_income_expense BOOLEAN, -- Part of other income/expense calculation
  is_net_income BOOLEAN, -- Part of net income calculation
  
  parent_account_key STRING,
  account_hierarchy_level INT,
  account_path STRING,
  is_summary BOOLEAN,
  is_inactive BOOLEAN,
  is_inventory BOOLEAN,
  is_bank_account BOOLEAN,
  is_balance_sheet BOOLEAN,
  is_income_statement BOOLEAN,
  
  -- Enhanced financial statement flags
  is_revenue_account BOOLEAN,
  is_cost_of_revenue_account BOOLEAN,
  is_expense_account BOOLEAN,
  is_other_income_account BOOLEAN,
  is_other_expense_account BOOLEAN,
  
  description STRING,
  subsidiary STRING,
  include_children STRING,
  eliminate STRING,
  revalue STRING,
  reconcile_with_matching STRING,
  bank_name STRING,
  bank_routing_number STRING,
  special_account STRING,
  external_id STRING,
  account_display_name STRING,
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold layer NetSuite accounts dimension with business classifications'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge from silver with business enhancements
MERGE INTO gold.finance.dim_account AS target
USING (
  SELECT
    sa.account_key,
    sa.account_id,
    sa.account_number,
    sa.account_name,
    sa.account_full_name,
    sa.account_type,
    sa.account_category,
    -- Enhanced subcategory classification - keep existing logic but add income statement logic
    CASE
      WHEN sa.account_category = 'Assets' THEN
        CASE
          WHEN UPPER(sa.account_type) LIKE '%CURRENT%' OR UPPER(sa.account_name) LIKE '%CURRENT%' THEN 'Current Assets'
          WHEN UPPER(sa.account_type) LIKE '%FIXED%' OR UPPER(sa.account_name) LIKE '%FIXED%' OR UPPER(sa.account_name) LIKE '%EQUIPMENT%' THEN 'Fixed Assets'
          WHEN UPPER(sa.account_name) LIKE '%CASH%' OR UPPER(sa.account_name) LIKE '%BANK%' THEN 'Cash & Cash Equivalents'
          WHEN UPPER(sa.account_name) LIKE '%RECEIVABLE%' THEN 'Accounts Receivable'
          WHEN UPPER(sa.account_name) LIKE '%INVENTORY%' THEN 'Inventory'
          ELSE 'Other Assets'
        END
      WHEN sa.account_category = 'Liabilities' THEN
        CASE
          WHEN UPPER(sa.account_type) LIKE '%CURRENT%' OR UPPER(sa.account_name) LIKE '%CURRENT%' THEN 'Current Liabilities'
          WHEN UPPER(sa.account_type) LIKE '%LONG%' OR UPPER(sa.account_name) LIKE '%LONG%' THEN 'Long-term Liabilities'
          WHEN UPPER(sa.account_name) LIKE '%PAYABLE%' THEN 'Accounts Payable'
          WHEN UPPER(sa.account_name) LIKE '%ACCRUED%' THEN 'Accrued Liabilities'
          ELSE 'Other Liabilities'
        END
      WHEN sa.account_category = 'Revenue' THEN
        CASE
          WHEN UPPER(sa.account_name) LIKE '%SALES%' OR UPPER(sa.account_name) LIKE '%REVENUE%' THEN 'Sales Revenue'
          WHEN UPPER(sa.account_name) LIKE '%INTEREST%' THEN 'Interest Income'
          WHEN UPPER(sa.account_name) LIKE '%OTHER%' THEN 'Other Income'
          ELSE 'Operating Revenue'
        END
      WHEN sa.account_category = 'Expenses' THEN
        CASE
          WHEN UPPER(sa.account_name) LIKE '%COST%' AND UPPER(sa.account_name) LIKE '%GOODS%' THEN 'Cost of Goods Sold'
          WHEN UPPER(sa.account_name) LIKE '%OPERATING%' THEN 'Operating Expenses'
          WHEN UPPER(sa.account_name) LIKE '%ADMIN%' OR UPPER(sa.account_name) LIKE '%GENERAL%' THEN 'General & Administrative'
          WHEN UPPER(sa.account_name) LIKE '%SALES%' AND UPPER(sa.account_name) LIKE '%MARKETING%' THEN 'Sales & Marketing'
          WHEN UPPER(sa.account_name) LIKE '%INTEREST%' THEN 'Interest Expense'
          ELSE 'Other Expenses'
        END
      ELSE sa.account_category
    END AS account_subcategory,
    
    -- Income statement classification from silver layer
    sa.transaction_type,
    sa.transaction_category,
    sa.transaction_subcategory,
    
    -- Income statement metric groupings from silver layer
    sa.revenue_metric_group,
    sa.cost_metric_group,
    sa.expense_metric_group,
    sa.other_metric_group,
    
    -- High-level P&L groupings from silver layer
    sa.is_total_revenue,
    sa.is_cost_of_revenue,
    sa.is_gross_profit,
    sa.is_operating_expense,
    sa.is_net_ordinary_revenue,
    sa.is_other_income_expense,
    sa.is_net_income,
    sa.parent_account_key,
    -- Calculate hierarchy level (simplified - would need recursive CTE for full hierarchy)
    CASE WHEN sa.parent_account_key IS NULL THEN 1 ELSE 2 END AS account_hierarchy_level,
    -- Create account path (simplified)
    COALESCE(sa.account_full_name, sa.account_name) AS account_path,
    sa.is_summary,
    sa.is_inactive,
    sa.is_inventory,
    -- Enhanced bank account detection
    CASE 
      WHEN sa.bank_name IS NOT NULL OR sa.bank_routing_number IS NOT NULL THEN TRUE
      WHEN UPPER(sa.account_type) LIKE '%BANK%' THEN TRUE
      WHEN UPPER(sa.account_name) LIKE '%BANK%' OR UPPER(sa.account_name) LIKE '%CASH%' THEN TRUE
      ELSE FALSE
    END AS is_bank_account,
    -- Balance sheet vs income statement classification
    CASE 
      WHEN sa.account_category IN ('Assets', 'Liabilities', 'Equity') THEN TRUE 
      ELSE FALSE 
    END AS is_balance_sheet,
    CASE 
      WHEN sa.account_category IN ('Revenue', 'Expenses') THEN TRUE 
      ELSE FALSE 
    END AS is_income_statement,
    
    -- Enhanced financial statement flags based on new transaction types
    (sa.transaction_type = 'REVENUE') AS is_revenue_account,
    (sa.transaction_type = 'COST_OF_REVENUE') AS is_cost_of_revenue_account,
    (sa.transaction_type = 'EXPENSE') AS is_expense_account,
    (sa.transaction_type = 'OTHER_INCOME') AS is_other_income_account,
    (sa.transaction_type = 'OTHER_EXPENSE') AS is_other_expense_account,
    sa.description,
    sa.subsidiary,
    sa.include_children,
    sa.eliminate,
    sa.revalue,
    sa.reconcile_with_matching,
    sa.bank_name,
    sa.bank_routing_number,
    sa.special_account,
    sa.external_id,
    -- Enhanced display name
    CASE
      WHEN sa.account_number IS NOT NULL AND sa.account_name IS NOT NULL THEN 
        CONCAT(sa.account_number, ' - ', sa.account_name)
      WHEN sa.account_number IS NOT NULL THEN sa.account_number
      ELSE COALESCE(sa.account_name, 'Unknown Account')
    END AS account_display_name,
    sa._source_table,
    CURRENT_TIMESTAMP() AS _load_timestamp
  FROM silver.finance.dim_account sa
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
    target.account_subcategory = source.account_subcategory,
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
    target.account_hierarchy_level = source.account_hierarchy_level,
    target.account_path = source.account_path,
    target.is_summary = source.is_summary,
    target.is_inactive = source.is_inactive,
    target.is_inventory = source.is_inventory,
    target.is_bank_account = source.is_bank_account,
    target.is_balance_sheet = source.is_balance_sheet,
    target.is_income_statement = source.is_income_statement,
    target.is_revenue_account = source.is_revenue_account,
    target.is_cost_of_revenue_account = source.is_cost_of_revenue_account,
    target.is_expense_account = source.is_expense_account,
    target.is_other_income_account = source.is_other_income_account,
    target.is_other_expense_account = source.is_other_expense_account,
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
    target.account_display_name = source.account_display_name,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    account_key,
    account_id,
    account_number,
    account_name,
    account_full_name,
    account_type,
    account_category,
    account_subcategory,
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
    account_hierarchy_level,
    account_path,
    is_summary,
    is_inactive,
    is_inventory,
    is_bank_account,
    is_balance_sheet,
    is_income_statement,
    is_revenue_account,
    is_cost_of_revenue_account,
    is_expense_account,
    is_other_income_account,
    is_other_expense_account,
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
    account_display_name,
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
    source.account_subcategory,
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
    source.account_hierarchy_level,
    source.account_path,
    source.is_summary,
    source.is_inactive,
    source.is_inventory,
    source.is_bank_account,
    source.is_balance_sheet,
    source.is_income_statement,
    source.is_revenue_account,
    source.is_cost_of_revenue_account,
    source.is_expense_account,
    source.is_other_income_account,
    source.is_other_expense_account,
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
    source.account_display_name,
    source._source_table,
    source._load_timestamp
  ); 
