# Income Statement Metric Groupings - Implementation Guide

## Overview

This implementation adds sophisticated income statement metric groupings to the data warehouse, making it extremely easy to filter by different revenue products, cost categories, and P&L line items. The solution aligns with your provided account summaries structure and enables powerful business intelligence reporting.

## What Was Added

### 1. Silver Layer Dimension (`models/silver/dim/dim_account.sql`)

**New Fields Added:**
- `revenue_metric_group` - Groups revenue accounts (REV_RESERVE, REV_VSC, REV_GAP, REV_DOC_FEES, etc.)
- `cost_metric_group` - Groups cost accounts (COR_DIRECT_PEOPLE_COST, COR_PAYOFF_EXPENSE, COR_REGISTRATION_EXPENSE, COR_OTHER)
- `expense_metric_group` - For future expense groupings
- `other_metric_group` - For future other groupings

**High-Level P&L Groupings:**
- `is_total_revenue` - All revenue accounts combined
- `is_cost_of_revenue` - All cost of revenue accounts combined
- `is_gross_profit` - Combination for gross profit calculation
- `is_operating_expense` - Operating expense accounts
- `is_net_ordinary_revenue` - Net ordinary revenue calculation
- `is_other_income_expense` - Other income/expense items
- `is_net_income` - Net income calculation

### 2. Gold Layer Dimension (`models/gold/dim/dim_account.sql`)

**Enhanced with all silver layer fields plus:**
- Inherits all metric grouping fields from silver layer
- Maintains existing enhanced boolean flags
- Provides clean interface for fact table joins

### 3. Gold Layer Fact Table (`models/gold/fact/fact_deal_netsuite_transactions.sql`)

**New Fields Added:**
- All metric grouping fields from dimension table via JOIN
- Enhanced filtering capabilities with boolean flags
- Improved business metrics calculations

## Account Groupings Implemented

### Revenue Metric Groups
```sql
-- Based on your account_summaries structure:
'REV_RESERVE'   → Accounts: 4105, 4106, 4107
'REV_VSC'       → Accounts: 4110, 4110A, 4110B, 4110C, 4111  
'REV_GAP'       → Accounts: 4120, 4120A, 4120B, 4120C, 4121
'REV_DOC_FEES'  → Accounts: 4130, 4130C
```

### Cost of Revenue Metric Groups
```sql
'COR_OTHER'                 → Accounts: 5110, 5110A, 5120A, 5120, 5199, 5510
'COR_DIRECT_PEOPLE_COST'    → Accounts: 5301, 5304, 5305, 5320, 5330, 5340
'COR_PAYOFF_EXPENSE'        → Accounts: 5400, 5520
'COR_REGISTRATION_EXPENSE'  → Accounts: 5402, 5141, 5530
```

### High-Level P&L Flags
```sql
is_total_revenue        → All 4000 series revenue accounts
is_cost_of_revenue      → All 5000 series cost accounts  
is_gross_profit         → Combination of revenue + cost accounts
```

## How to Use for Easy Filtering

### 1. **Revenue Analysis by Product**
```sql
-- Filter by specific revenue products
WHERE d.revenue_metric_group = 'REV_VSC'

-- Filter by multiple revenue products  
WHERE d.revenue_metric_group IN ('REV_RESERVE', 'REV_VSC')

-- Filter by all revenue
WHERE d.is_total_revenue = TRUE
```

### 2. **Cost Analysis by Category**
```sql
-- Filter by specific cost category
WHERE d.cost_metric_group = 'COR_DIRECT_PEOPLE_COST'

-- Filter by multiple cost categories
WHERE d.cost_metric_group IN ('COR_PAYOFF_EXPENSE', 'COR_REGISTRATION_EXPENSE')

-- Filter by all cost of revenue
WHERE d.is_cost_of_revenue = TRUE
```

### 3. **P&L Line Item Analysis**
```sql
-- Gross profit calculation
WHERE d.is_gross_profit = TRUE

-- Revenue vs Cost comparison
WHERE d.is_total_revenue = TRUE OR d.is_cost_of_revenue = TRUE
```

## Hex Dashboard Implementation

### Suggested Parameters
```javascript
// Hex dashboard parameters for easy filtering:
{{ year_filter }}           // Dropdown: 2022, 2023, 2024, etc.
{{ start_month }}           // Dropdown: 1-12
{{ end_month }}             // Dropdown: 1-12
{{ revenue_products }}      // Multi-select: REV_RESERVE, REV_VSC, REV_GAP, REV_DOC_FEES  
{{ cost_categories }}       // Multi-select: COR_DIRECT_PEOPLE_COST, COR_PAYOFF_EXPENSE, etc.
{{ include_credit_memos }}  // Boolean: TRUE/FALSE
```

### Example Filter Clauses
```sql
-- Product-specific revenue filter
AND d.revenue_metric_group IN {{ revenue_products }}

-- Cost category filter  
AND d.cost_metric_group IN {{ cost_categories }}

-- Credit memo handling
AND ({{ include_credit_memos }} = TRUE OR f.has_credit_memo = FALSE)

-- Date range filter
AND f.month BETWEEN {{ start_month }} AND {{ end_month }}
```

## Key Benefits

### 1. **Simplified Filtering**
- No need to remember specific account numbers
- Logical business groupings (REV_VSC, COR_PAYOFF_EXPENSE, etc.)
- Boolean flags for high-level P&L analysis

### 2. **Consistent Reporting**
- Standardized metric definitions across all reports
- Aligned with income statement structure
- Easy aggregation and drill-down capabilities

### 3. **Business User Friendly**
- Finance terms that business users understand
- No technical account number knowledge required
- Self-service analytics capabilities

### 4. **Flexible Analysis**
- Mix and match different metric groups
- Easy trend analysis by product/category
- Deal-level profitability analysis

## Example Use Cases

### 1. **Monthly Revenue by Product**
```sql
SELECT 
  f.year, f.month,
  d.revenue_metric_group,
  SUM(f.amount_dollars) as revenue
FROM gold.finance.fact_deal_netsuite_transactions f
JOIN gold.finance.dim_account d ON f.account_key = d.account_key  
WHERE d.is_total_revenue = TRUE
GROUP BY f.year, f.month, d.revenue_metric_group;
```

### 2. **Cost Breakdown Analysis**
```sql
SELECT 
  d.cost_metric_group,
  SUM(f.amount_dollars) as total_cost,
  COUNT(DISTINCT f.deal_key) as deal_count
FROM gold.finance.fact_deal_netsuite_transactions f
JOIN gold.finance.dim_account d ON f.account_key = d.account_key
WHERE d.is_cost_of_revenue = TRUE
GROUP BY d.cost_metric_group;
```

### 3. **Gross Profit Calculation**
```sql
SELECT 
  f.year, f.month,
  SUM(CASE WHEN d.is_total_revenue = TRUE THEN f.amount_dollars ELSE 0 END) as total_revenue,
  SUM(CASE WHEN d.is_cost_of_revenue = TRUE THEN f.amount_dollars ELSE 0 END) as total_costs,
  (SUM(CASE WHEN d.is_total_revenue = TRUE THEN f.amount_dollars ELSE 0 END) - 
   SUM(CASE WHEN d.is_cost_of_revenue = TRUE THEN f.amount_dollars ELSE 0 END)) as gross_profit
FROM gold.finance.fact_deal_netsuite_transactions f
JOIN gold.finance.dim_account d ON f.account_key = d.account_key
WHERE d.is_gross_profit = TRUE
GROUP BY f.year, f.month;
```

## Files Modified

1. **`models/silver/dim/dim_account.sql`** - Added metric grouping logic
2. **`models/gold/dim/dim_account.sql`** - Enhanced with silver layer fields  
3. **`models/gold/fact/fact_deal_netsuite_transactions.sql`** - Added metric fields via dimension JOIN
4. **`hex_query_examples.sql`** - Comprehensive query examples for Hex
5. **`INCOME_STATEMENT_GROUPINGS_GUIDE.md`** - This implementation guide

## Next Steps

1. **Deploy to Databricks**: Run the updated SQL files to implement the changes
2. **Test Data Quality**: Verify metric groupings are correctly assigned
3. **Create Hex Dashboards**: Use the example queries to build finance dashboards
4. **Train Users**: Share this guide with finance team for self-service analytics
5. **Monitor Performance**: Ensure queries perform well with new grouping fields

## Maintenance

- **Adding New Accounts**: Update the account mapping logic in silver layer
- **New Metric Groups**: Add new grouping fields following the same pattern
- **Business Logic Changes**: Modify grouping rules in silver dimension table

This implementation provides a robust foundation for finance reporting with intuitive, business-friendly filtering capabilities that align perfectly with your income statement structure. 