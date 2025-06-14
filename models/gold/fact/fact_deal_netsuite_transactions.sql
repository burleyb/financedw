-- models/gold/fact/fact_deal_netsuite_transactions.sql
-- Gold layer NetSuite transactions fact table with enhanced business logic

-- Drop and recreate table to ensure correct schema
DROP TABLE IF EXISTS gold.finance.fact_deal_netsuite_transactions;

-- 1. Create the enhanced gold fact table
CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_netsuite_transactions (
  transaction_key STRING NOT NULL,
  deal_key STRING NOT NULL,
  account_key STRING NOT NULL, -- Foreign key to gold.finance.dim_account
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
  fiscal_month INT,
  fiscal_quarter INT,
  fiscal_year INT,
  
  -- Enhanced transaction details
  transaction_type STRING,
  transaction_category STRING,
  transaction_subcategory STRING,
  transaction_group STRING, -- Business grouping (e.g., 'PRODUCT_REVENUE', 'COST_OF_REVENUE', 'OPERATING_EXPENSE')
  is_revenue BOOLEAN,
  is_expense BOOLEAN,
  is_cost_of_revenue BOOLEAN,
  is_operating_expense BOOLEAN,
  
  -- Financial amounts
  amount_cents BIGINT,
  amount_dollars DECIMAL(15,2),
  amount_dollars_abs DECIMAL(15,2), -- Absolute value for easier aggregation
  
  -- Allocation and data quality
  allocation_method STRING,
  allocation_factor DECIMAL(10,6),
  data_quality_score DECIMAL(3,2), -- 0.0 to 1.0 score based on allocation method and completeness
  
  -- Business metrics
  revenue_per_deal DECIMAL(15,2),
  expense_per_deal DECIMAL(15,2),
  profit_contribution DECIMAL(15,2), -- Revenue - Expenses for this transaction
  
  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP,
  _gold_processed_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (fiscal_year, fiscal_quarter)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes from silver layer
MERGE INTO gold.finance.fact_deal_netsuite_transactions AS target
USING (
  WITH fiscal_calendar AS (
    -- Define fiscal year logic (assuming fiscal year starts in January)
    SELECT
      month,
      year,
      month as fiscal_month,
      CASE 
        WHEN month <= 3 THEN 1
        WHEN month <= 6 THEN 2
        WHEN month <= 9 THEN 3
        ELSE 4
      END as fiscal_quarter,
      year as fiscal_year
    FROM (
      SELECT DISTINCT month, year 
      FROM silver.finance.fact_deal_netsuite_transactions
    )
  ),
  
  enhanced_transactions AS (
    SELECT
      sf.transaction_key,
      sf.deal_key,
      sf.account_key,
      sf.netsuite_posting_date_key,
      sf.netsuite_posting_time_key,
      sf.revenue_recognition_date_key,
      sf.revenue_recognition_time_key,
      sf.vin,
      sf.month,
      sf.year,
      fc.fiscal_month,
      fc.fiscal_quarter,
      fc.fiscal_year,
      
      -- Enhanced transaction details
      sf.transaction_type,
      sf.transaction_category,
      sf.transaction_subcategory,
      
      -- Business grouping logic
      CASE 
        WHEN sf.transaction_type = 'REVENUE' THEN 'PRODUCT_REVENUE'
        WHEN sf.transaction_type = 'EXPENSE' AND sf.transaction_category IN ('VSC_COR', 'GAP_COR') THEN 'COST_OF_REVENUE'
        WHEN sf.transaction_type = 'EXPENSE' THEN 'OPERATING_EXPENSE'
        ELSE 'OTHER'
      END as transaction_group,
      
      -- Boolean flags for easier filtering
      (sf.transaction_type = 'REVENUE') as is_revenue,
      (sf.transaction_type = 'EXPENSE') as is_expense,
      (sf.transaction_type = 'EXPENSE' AND sf.transaction_category IN ('VSC_COR', 'GAP_COR')) as is_cost_of_revenue,
      (sf.transaction_type = 'EXPENSE' AND sf.transaction_category NOT IN ('VSC_COR', 'GAP_COR')) as is_operating_expense,
      
      -- Financial amounts
      sf.amount_cents,
      sf.amount_dollars,
      ABS(sf.amount_dollars) as amount_dollars_abs,
      
      -- Allocation and data quality
      sf.allocation_method,
      sf.allocation_factor,
      
      -- Data quality score based on allocation method
      CASE sf.allocation_method
        WHEN 'VIN_MATCH' THEN 1.0
        WHEN 'PERIOD_ALLOCATION' THEN 0.7
        WHEN 'DIRECT' THEN 0.9
        ELSE 0.5
      END as data_quality_score,
      
      -- Profit contribution (positive for revenue, negative for expenses)
      CASE 
        WHEN sf.transaction_type = 'REVENUE' THEN sf.amount_dollars
        WHEN sf.transaction_type = 'EXPENSE' THEN -sf.amount_dollars
        ELSE 0
      END as profit_contribution,
      
      sf._source_table,
      sf._load_timestamp,
      
      -- Credit memo flags
      sf.has_credit_memo,
      sf.credit_memo_date_key,
      sf.credit_memo_time_key
    FROM silver.finance.fact_deal_netsuite_transactions sf
    INNER JOIN fiscal_calendar fc ON sf.month = fc.month AND sf.year = fc.year
    -- Ensure account exists in gold dimension
    INNER JOIN gold.finance.dim_account da ON sf.account_key = da.account_key
  ),
  
  -- Calculate per-deal metrics
  deal_metrics AS (
    SELECT
      deal_key,
      SUM(CASE WHEN is_revenue THEN amount_dollars ELSE 0 END) as total_revenue_per_deal,
      SUM(CASE WHEN is_expense THEN amount_dollars ELSE 0 END) as total_expense_per_deal
    FROM enhanced_transactions
    GROUP BY deal_key
  ),
  
  final_enhanced AS (
    SELECT
      et.*,
      dm.total_revenue_per_deal as revenue_per_deal,
      dm.total_expense_per_deal as expense_per_deal,
      CURRENT_TIMESTAMP() as _gold_processed_timestamp
    FROM enhanced_transactions et
    LEFT JOIN deal_metrics dm ON et.deal_key = dm.deal_key
  )
  
  SELECT * FROM final_enhanced

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
    target.fiscal_month = source.fiscal_month,
    target.fiscal_quarter = source.fiscal_quarter,
    target.fiscal_year = source.fiscal_year,
    target.transaction_type = source.transaction_type,
    target.transaction_category = source.transaction_category,
    target.transaction_subcategory = source.transaction_subcategory,
    target.transaction_group = source.transaction_group,
    target.is_revenue = source.is_revenue,
    target.is_expense = source.is_expense,
    target.is_cost_of_revenue = source.is_cost_of_revenue,
    target.is_operating_expense = source.is_operating_expense,
    target.amount_cents = source.amount_cents,
    target.amount_dollars = source.amount_dollars,
    target.amount_dollars_abs = source.amount_dollars_abs,
    target.allocation_method = source.allocation_method,
    target.allocation_factor = source.allocation_factor,
    target.data_quality_score = source.data_quality_score,
    target.revenue_per_deal = source.revenue_per_deal,
    target.expense_per_deal = source.expense_per_deal,
    target.profit_contribution = source.profit_contribution,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp,
    target.has_credit_memo = source.has_credit_memo,
    target.credit_memo_date_key = source.credit_memo_date_key,
    target.credit_memo_time_key = source.credit_memo_time_key,
    target._gold_processed_timestamp = source._gold_processed_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    transaction_key, deal_key, account_key, netsuite_posting_date_key, netsuite_posting_time_key,
    revenue_recognition_date_key, revenue_recognition_time_key, vin, month, year,
    fiscal_month, fiscal_quarter, fiscal_year, transaction_type, transaction_category, transaction_subcategory,
    transaction_group, is_revenue, is_expense, is_cost_of_revenue, is_operating_expense,
    amount_cents, amount_dollars, amount_dollars_abs, allocation_method, allocation_factor,
    data_quality_score, revenue_per_deal, expense_per_deal, profit_contribution,
    _source_table, _load_timestamp, has_credit_memo, credit_memo_date_key, credit_memo_time_key, _gold_processed_timestamp
  )
  VALUES (
    source.transaction_key, source.deal_key, source.account_key, source.netsuite_posting_date_key, source.netsuite_posting_time_key,
    source.revenue_recognition_date_key, source.revenue_recognition_time_key, source.vin, source.month, source.year,
    source.fiscal_month, source.fiscal_quarter, source.fiscal_year, source.transaction_type, source.transaction_category, source.transaction_subcategory,
    source.transaction_group, source.is_revenue, source.is_expense, source.is_cost_of_revenue, source.is_operating_expense,
    source.amount_cents, source.amount_dollars, source.amount_dollars_abs, source.allocation_method, source.allocation_factor,
    source.data_quality_score, source.revenue_per_deal, source.expense_per_deal, source.profit_contribution,
    source._source_table, source._load_timestamp, source.has_credit_memo, source.credit_memo_date_key, source.credit_memo_time_key, source._gold_processed_timestamp
  );

-- 3. Create optimized indexes and statistics
-- Note: Z-ORDER cannot include partition columns (fiscal_year, fiscal_quarter)
OPTIMIZE gold.finance.fact_deal_netsuite_transactions ZORDER BY (deal_key, account_key, transaction_type, vin);

-- 4. Update table statistics for query optimization
ANALYZE TABLE gold.finance.fact_deal_netsuite_transactions COMPUTE STATISTICS; 