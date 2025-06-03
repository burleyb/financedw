-- models/silver/dim/dim_account.sql
-- Dimension table for NetSuite accounts, sourced from bronze.ns.account
-- Drop and recreate table to ensure correct schema
DROP TABLE IF EXISTS silver.finance.dim_account;

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS silver.finance.dim_account (
  account_key STRING NOT NULL, -- Natural key (account ID)
  account_id BIGINT, -- Original NetSuite account ID
  account_number STRING, -- Account number
  account_name STRING, -- Account display name
  account_full_name STRING, -- Full hierarchical name
  account_type STRING, -- Account type
  account_category STRING, -- Business category classification
  parent_account_id BIGINT, -- Parent account for hierarchy
  is_summary BOOLEAN, -- Whether this is a summary account
  is_inactive BOOLEAN, -- Whether the account is inactive
  is_inventory BOOLEAN, -- Whether this is an inventory account
  description STRING, -- Account description
  subsidiary STRING, -- Subsidiary information
  include_children STRING, -- Include children flag
  eliminate STRING, -- Elimination flag
  revalue STRING, -- Revalue flag
  reconcile_with_matching STRING, -- Reconciliation flag
  bank_name STRING, -- Bank name if applicable
  bank_routing_number STRING, -- Bank routing number if applicable
  special_account STRING, -- Special account designation
  external_id STRING, -- External ID
  _source_table STRING, -- Metadata: Originating source table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
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
  -- Select the latest distinct account data from the bronze NetSuite accounts table
  SELECT  
    CAST(a.id AS STRING) AS account_key, -- Natural Key
    a.id as account_id,
    a.acctnumber AS account_number,
    COALESCE(a.accountsearchdisplayname, a.fullname, 'Unknown Account') AS account_name,
    a.fullname AS account_full_name,
    a.accttype AS account_type,
    -- Categorize accounts based on type patterns
    CASE
      WHEN UPPER(a.accttype) LIKE '%ASSET%' THEN 'Assets'
      WHEN UPPER(a.accttype) LIKE '%LIABILITY%' THEN 'Liabilities'
      WHEN UPPER(a.accttype) LIKE '%EQUITY%' THEN 'Equity'
      WHEN UPPER(a.accttype) LIKE '%INCOME%' OR UPPER(a.accttype) LIKE '%REVENUE%' THEN 'Revenue'
      WHEN UPPER(a.accttype) LIKE '%EXPENSE%' OR UPPER(a.accttype) LIKE '%COST%' THEN 'Expenses'
      WHEN UPPER(a.accttype) LIKE '%BANK%' THEN 'Bank'
      ELSE 'Other'
    END AS account_category,
    a.parent AS parent_account_id,
    CASE WHEN UPPER(a.issummary) = 'T' THEN TRUE ELSE FALSE END AS is_summary,
    CASE WHEN UPPER(a.isinactive) = 'T' THEN TRUE ELSE FALSE END AS is_inactive,
    CASE WHEN UPPER(a.inventory) = 'T' THEN TRUE ELSE FALSE END AS is_inventory,
    a.description,
    a.subsidiary,
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
    target.parent_account_id <> source.parent_account_id OR
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
    target.parent_account_id = source.parent_account_id,
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
    target._source_table = source._source_table,
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
    parent_account_id,
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
    source.parent_account_id,
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
  SELECT 'No Account' as account_key, NULL as account_id, NULL as account_number, 'No Account' as account_name, 'No Account' as account_full_name, 'Other' as account_type, 'Other' as account_category, NULL as parent_account_id, false as is_summary, false as is_inactive, false as is_inventory, 'Default account for null values' as description, NULL as subsidiary, NULL as include_children, NULL as eliminate, NULL as revalue, NULL as reconcile_with_matching, NULL as bank_name, NULL as bank_routing_number, NULL as special_account, NULL as external_id, 'static' as _source_table
  UNION ALL
  SELECT 'Unknown' as account_key, NULL as account_id, NULL as account_number, 'Unknown' as account_name, 'Unknown' as account_full_name, 'Other' as account_type, 'Other' as account_category, NULL as parent_account_id, false as is_summary, false as is_inactive, false as is_inventory, 'Default account for unknown values' as description, NULL as subsidiary, NULL as include_children, NULL as eliminate, NULL as revalue, NULL as reconcile_with_matching, NULL as bank_name, NULL as bank_routing_number, NULL as special_account, NULL as external_id, 'static' as _source_table
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
    parent_account_id,
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
    source.parent_account_id,
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