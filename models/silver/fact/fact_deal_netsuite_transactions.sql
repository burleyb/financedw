-- models/silver/fact/fact_deal_netsuite_transactions.sql
-- Normalized NetSuite fact table with account foreign keys for flexible pivoting

-- Drop and recreate table to ensure correct schema
DROP TABLE IF EXISTS silver.finance.fact_deal_netsuite_transactions;

-- 1. Create the normalized fact table
CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_netsuite_transactions (
  transaction_key STRING NOT NULL, -- Unique key: deal_key + account_key + transaction_type
  deal_key STRING NOT NULL,
  account_key STRING NOT NULL, -- Foreign key to dim_account
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
        DATE_FORMAT(ds.updated_date_utc, 'yyyy-MM') as revenue_recognition_period,
        YEAR(ds.updated_date_utc) as revenue_recognition_year,
        MONTH(ds.updated_date_utc) as revenue_recognition_month,
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
  
  -- Account mapping configuration
  account_mappings AS (
    SELECT * FROM VALUES
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
  
  -- Missing VIN transactions by period (for allocation)
  missing_vin_revenue AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        so.account,
        SUM(so.amount) AS total_amount
    FROM bronze.ns.salesinvoiced AS so
    INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    INNER JOIN account_mappings am ON so.account = am.account_id
    WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
        AND am.transaction_type = 'REVENUE'
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), so.account
  ),
  
  missing_vin_expenses AS (
    SELECT
        DATE_FORMAT(t.trandate, 'yyyy-MM') as transaction_period,
        tl.expenseaccount as account,
        SUM(tl.foreignamount) AS total_amount
    FROM bronze.ns.transactionline AS tl
    INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    INNER JOIN account_mappings am ON tl.expenseaccount = am.account_id
    WHERE (LENGTH(t.custbody_leaseend_vinno) != 17 OR t.custbody_leaseend_vinno IS NULL)
        AND t.trandate IS NOT NULL
        AND am.transaction_type = 'EXPENSE'
        AND t.abbrevtype NOT IN ('PURCHORD', 'SALESORD', 'RTN AUTH', 'VENDAUTH')
        AND (t.approvalstatus = 2 OR t.approvalstatus IS NULL)
    GROUP BY DATE_FORMAT(t.trandate, 'yyyy-MM'), tl.expenseaccount
  ),
  
  -- Combine VIN-matching and allocated amounts
  final_transactions AS (
    -- VIN-matching revenue transactions
    SELECT
        CONCAT(rrwv.deal_id, '_', vmr.account, '_REVENUE') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(vmr.account AS STRING) as account_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS INT), 0) AS revenue_recognition_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS INT), 0) AS revenue_recognition_time_key,
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
        CAST(vme.account AS STRING) as account_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
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
        'bronze.ns.transactionline' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN vin_matching_expenses vme ON rrwv.vin = vme.vin
    INNER JOIN account_mappings am ON vme.account = am.account_id
    
    UNION ALL
    
    -- Allocated missing VIN revenue transactions
    SELECT
        CONCAT(rrwv.deal_id, '_', mvr.account, '_REVENUE_ALLOCATED') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(mvr.account AS STRING) as account_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
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
        'bronze.ns.salesinvoiced' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    INNER JOIN missing_vin_revenue mvr ON rrwv.revenue_recognition_period = mvr.transaction_period
    INNER JOIN account_mappings am ON mvr.account = am.account_id
    
    UNION ALL
    
    -- Allocated missing VIN expense transactions
    SELECT
        CONCAT(rrwv.deal_id, '_', mve.account, '_EXPENSE_ALLOCATED') as transaction_key,
        CAST(rrwv.deal_id AS STRING) as deal_key,
        CAST(mve.account AS STRING) as account_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'yyyyMMdd') AS BIGINT), 0) AS netsuite_posting_date_key,
        COALESCE(CAST(DATE_FORMAT(rrwv.revenue_recognition_date_utc, 'HHmmss') AS BIGINT), 0) AS netsuite_posting_time_key,
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
        'bronze.ns.transactionline' as _source_table
    FROM revenue_recognition_with_vins rrwv
    INNER JOIN deals_per_period dpp ON rrwv.revenue_recognition_period = dpp.revenue_recognition_period
    INNER JOIN missing_vin_expenses mve ON rrwv.revenue_recognition_period = mve.transaction_period
    INNER JOIN account_mappings am ON mve.account = am.account_id
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