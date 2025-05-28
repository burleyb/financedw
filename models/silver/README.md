# Silver Layer Data Models

This directory contains fact and dimension tables that source directly from bronze tables to create the silver layer of the medallion architecture. This approach provides several advantages over the existing gold_big_deal approach.

## Medallion Architecture

This project follows the medallion architecture pattern:

- **Bronze Layer**: Raw data ingested from source systems (bronze.leaseend_db_public.*)
- **Silver Layer**: Cleaned, validated, and enriched data (silver.finance.*)
- **Gold Layer**: Business-level aggregated data optimized for analytics (gold.finance.*)

## Benefits of Silver Layer Approach

1. **Data Lineage Transparency**: Clear visibility into which bronze tables contribute to each field
2. **Reduced Dependencies**: No reliance on the silver.deal.big_deal table which may have inconsistencies
3. **Granular Control**: Ability to apply specific business logic and data quality rules at the source level
4. **Performance**: Better performance by avoiding intermediate aggregation layers
5. **Debugging**: Easier to trace data issues back to their source systems
6. **Scalability**: Follows industry best practices for data warehouse architecture

## Table Structure

### Fact Tables
- `fact_deals.sql`: Main deals fact table with financial measures sourced from bronze tables

### Dimension Tables  
- `dim_deal.sql`: Deal dimension with attributes sourced from bronze tables

## Data Sources

The silver layer sources directly from:
- `bronze.leaseend_db_public.deals`
- `bronze.leaseend_db_public.financial_infos`
- `bronze.leaseend_db_public.cars`
- `bronze.leaseend_db_public.deal_states`
- `bronze.leaseend_db_public.deals_referrals_sources`
- `bronze.leaseend_db_public.payoffs`

## Business Logic Implementation

The silver layer replicates the logic from `bigdealleaseendcreation.py` in SQL:

1. **Latest Record Selection**: Uses ROW_NUMBER() window functions to get the most recent records from each bronze table
2. **Calculated Fields**: 
   - `ally_fees`: Calculated based on option_type (vsc=675, gap=50, vscPlusGap=725)
   - `vsc_rev`: vsc_price - vsc_cost for relevant option types
   - `gap_rev`: gap_price - gap_cost for relevant option types
   - `rpt`: title + doc_fee + profit + ally_fees
3. **Data Quality**: Filters out deleted records using `_fivetran_deleted` flags

## Additional Attributes

The silver dimension table includes additional attributes not available in the gold_big_deal approach:
- `imported_date_utc`
- `auto_import_variation`
- `paperwork_type`
- `signing_on_com`
- `marketing_source`
- `sales_visibility`
- `has_problem`
- `docs_sent_date`
- `needs_electronic_signature_verification`
- `lease_id`
- `boot_reason`
- `import_type`

## Usage

These tables can be used alongside the existing gold_big_deal tables for comparison and validation:

```sql
-- Compare record counts
SELECT 'gold_big_deal' as source, COUNT(*) as record_count 
FROM gold.finance.fact_deals
UNION ALL
SELECT 'silver' as source, COUNT(*) as record_count 
FROM silver.finance.fact_deals;

-- Compare specific measures for the same deals
SELECT 
  g.deal_key,
  g.profit_amount as gold_profit,
  s.profit_amount as silver_profit,
  g.vsc_rev_amount as gold_vsc_rev,
  s.vsc_rev_amount as silver_vsc_rev
FROM gold.finance.fact_deals g
JOIN silver.finance.fact_deals s ON g.deal_key = s.deal_key
WHERE g.profit_amount != s.profit_amount 
   OR g.vsc_rev_amount != s.vsc_rev_amount;
```

## Maintenance

The silver tables should be refreshed before the gold layer tables. Consider:

1. **Incremental Updates**: Both approaches use MERGE statements for efficient incremental processing
2. **Data Quality Monitoring**: Monitor for discrepancies between silver and gold_big_deal approaches
3. **Performance Optimization**: The silver approach may require additional indexing due to multiple table joins

## Migration Strategy

1. **Parallel Operation**: Run both approaches in parallel initially
2. **Validation**: Compare results between silver and gold_big_deal approaches using validation_queries.sql
3. **Gold Layer Creation**: Create gold layer tables that source from silver tables
4. **Gradual Migration**: Migrate downstream consumers gradually after validation
5. **Deprecation**: Deprecate gold_big_deal tables once silver→gold pipeline is proven stable

## Next Steps

1. Create gold layer tables that source from these silver tables
2. Implement additional business rules and aggregations in the gold layer
3. Optimize performance with appropriate partitioning and indexing
4. Set up automated data quality monitoring 