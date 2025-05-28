# Gold Layer Data Models

This directory contains the final business-ready fact and dimension tables that source from the silver layer. The gold layer represents the consumption layer of the medallion architecture, optimized for analytics, reporting, and business intelligence.

## Medallion Architecture - Gold Layer

The gold layer is the final stage in the medallion architecture:

- **Bronze Layer**: Raw data ingested from source systems
- **Silver Layer**: Cleaned, validated, and enriched data
- **Gold Layer**: Business-ready, aggregated data optimized for analytics and reporting

## Purpose of Gold Layer

1. **Business-Ready Data**: Tables are optimized for business users and analytics tools
2. **Calculated Metrics**: Pre-calculated KPIs and business metrics for performance
3. **Data Quality**: Additional data quality flags and business rules applied
4. **Performance Optimization**: Optimized for query performance and BI tools
5. **Business Classifications**: Enhanced with business logic and categorizations

## Table Structure

### Fact Tables
- `fact_deals.sql`: Business-ready deals fact table with calculated metrics and KPIs

### Dimension Tables  
- `dim_deal.sql`: Deal dimension with business classifications and enhanced attributes

## Gold Layer Enhancements

### Calculated Business Metrics (Fact Table)
- `total_revenue_amount`: Sum of all revenue streams (vsc_rev + gap_rev + profit + ally_fees)
- `total_cost_amount`: Sum of all costs (vsc_cost + gap_cost)
- `gross_margin_amount`: Total revenue minus total costs
- `margin_percentage`: Calculated margin percentage with division-by-zero protection

### Data Quality Flags (Fact Table)
- `is_complete_deal`: Boolean flag indicating deals with complete financial data
- `has_financial_products`: Boolean flag for deals with VSC or GAP products

### Business Classifications (Dimension Table)
- `deal_category`: Categorizes deals by type and products:
  - Full Protection Package (vscPlusGap)
  - VSC Only, GAP Only
  - Lease Only, Purchase Only
- `processing_complexity`: Classifies processing complexity:
  - Simple: Basic deals with no complications
  - Standard: Deals with standard products or options
  - Complex: Deals with problems, special requirements, or complications
- `customer_segment`: Segments customers based on acquisition channel:
  - Premium: VIP or premium marketing sources
  - Referral: Referral-based customers
  - Digital: Online/digital acquisition
  - Marketing: Other marketing channels
  - Standard: Default segment

## Data Sources

The gold layer sources exclusively from silver layer tables:
- `silver.finance.fact_deals`
- `silver.finance.dim_deal`

## Usage Examples

### Business Metrics Analysis
```sql
-- Revenue analysis with calculated metrics
SELECT 
  deal_category,
  COUNT(*) as deal_count,
  SUM(total_revenue_amount) / 100.0 as total_revenue,
  AVG(margin_percentage) as avg_margin_pct,
  SUM(CASE WHEN is_complete_deal THEN 1 ELSE 0 END) as complete_deals
FROM gold.finance.fact_deals f
JOIN gold.finance.dim_deal d ON f.deal_key = d.deal_key
WHERE f.creation_date_key >= 20240101
GROUP BY deal_category
ORDER BY total_revenue DESC;
```

### Customer Segmentation Analysis
```sql
-- Customer segment performance
SELECT 
  customer_segment,
  processing_complexity,
  COUNT(*) as deal_count,
  AVG(total_revenue_amount) / 100.0 as avg_revenue_per_deal,
  SUM(CASE WHEN has_financial_products THEN 1 ELSE 0 END) as deals_with_products
FROM gold.finance.fact_deals f
JOIN gold.finance.dim_deal d ON f.deal_key = d.deal_key
GROUP BY customer_segment, processing_complexity
ORDER BY customer_segment, processing_complexity;
```

### Data Quality Monitoring
```sql
-- Data quality overview
SELECT 
  COUNT(*) as total_deals,
  SUM(CASE WHEN is_complete_deal THEN 1 ELSE 0 END) as complete_deals,
  SUM(CASE WHEN has_financial_products THEN 1 ELSE 0 END) as deals_with_products,
  AVG(margin_percentage) as avg_margin,
  COUNT(CASE WHEN margin_percentage > 50 THEN 1 END) as high_margin_deals
FROM gold.finance.fact_deals
WHERE creation_date_key >= 20240101;
```

## Performance Optimizations

1. **Partitioning**: Tables are partitioned by `creation_date_key` for efficient date-based queries
2. **Auto-Optimization**: Delta Lake auto-optimization enabled for write and compaction
3. **Pre-Calculated Metrics**: Business metrics are pre-calculated to avoid complex joins in BI tools
4. **Indexing**: Consider Z-ordering on frequently filtered columns

## Data Refresh Strategy

1. **Dependencies**: Gold layer depends on silver layer completion
2. **Frequency**: Should be refreshed after silver layer updates
3. **Incremental**: Uses MERGE operations for efficient incremental updates
4. **Monitoring**: Monitor for data quality issues and calculation accuracy

## Business Rules

### Revenue Calculation
- Total revenue includes all income streams: VSC revenue, GAP revenue, profit, and ally fees
- Margin percentage calculation includes division-by-zero protection

### Deal Classification
- Deal categories are mutually exclusive based on option_type_key
- Processing complexity considers multiple factors: problems, special requirements, product complexity
- Customer segments are based on marketing source and referral patterns

## Integration with BI Tools

The gold layer is optimized for:
- **Hex**: Direct connection for dashboards and analytics
- **Tableau/Power BI**: Star schema design for efficient reporting
- **SQL Analytics**: Business-friendly column names and pre-calculated metrics
- **Data Science**: Clean, consistent data for modeling and analysis

## Maintenance

1. **Schema Evolution**: Use Delta Lake schema evolution for adding new calculated fields
2. **Data Quality**: Monitor calculated metrics for accuracy
3. **Performance**: Regular optimization of partition pruning and Z-ordering
4. **Documentation**: Keep business logic documentation updated as rules evolve 