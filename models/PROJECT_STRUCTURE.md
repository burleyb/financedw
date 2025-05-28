# Finance Data Warehouse Project Structure

This document outlines the current project structure after reorganizing to follow the medallion architecture pattern.

## Directory Structure

```
models/
├── gold_big_deal/          # EXISTING gold tables (sourced from silver.deal.big_deal)
│   ├── dim/
│   │   └── dim_deal.sql    # Existing dimension table
│   └── fact/
│       └── fact_deals.sql  # Existing fact table
├── silver/                 # NEW silver layer (sourced from bronze tables)
│   ├── dim/
│   │   └── dim_deal.sql    # Silver dimension table
│   ├── fact/
│   │   └── fact_deals.sql  # Silver fact table
│   ├── README.md           # Silver layer documentation
│   └── validation_queries.sql # Validation queries for all layers
└── gold/                   # NEW gold layer (sourced from silver layer)
    ├── dim/
    │   └── dim_deal.sql    # Gold dimension table
    ├── fact/
    │   └── fact_deals.sql  # Gold fact table
    └── README.md           # Gold layer documentation
```

## Medallion Architecture Implementation

### Bronze Layer
- **Location**: `bronze.leaseend_db_public.*` (Databricks tables)
- **Purpose**: Raw data ingested from source systems
- **Key Tables**: 
  - `deals` - Core deal information
  - `financial_infos` - Financial details and calculations
  - `cars` - Vehicle information
  - `customers` - Customer data
  - `deal_states` - Deal state tracking

### Silver Layer
- **Location**: `silver.finance.*` (New implementation)
- **Purpose**: Cleaned, validated, and enriched data sourced directly from bronze
- **Tables**:
  - `silver.finance.fact_deals` - Deal metrics and measures
  - `silver.finance.dim_deal` - Deal attributes and dimensions
- **Schema**: Mirrors the existing gold_big_deal schema exactly
- **Source**: Bronze tables with complex joins and business logic

### Gold Layer (Existing - gold_big_deal)
- **Location**: `gold.finance.*` (Current production tables)
- **Purpose**: Business-ready data for analytics and reporting
- **Tables**:
  - `gold.finance.fact_deals` - Current production fact table
  - `gold.finance.dim_deal` - Current production dimension table
- **Source**: `silver.deal.big_deal` table

### Gold Layer (New)
- **Location**: `gold.finance.*` (New implementation - same schema name)
- **Purpose**: Business-ready data sourced from new silver layer
- **Tables**:
  - `gold.finance.fact_deals` - New fact table (same name as existing)
  - `gold.finance.dim_deal` - New dimension table (same name as existing)
- **Source**: New silver layer tables
- **Schema**: Identical to existing gold_big_deal tables

## Schema Consistency

All layers (silver, gold_big_deal, gold_new) use the **exact same schema**:

### Fact Table Schema
- **Keys**: deal_key, deal_state_key, deal_type_key, driver_key, vehicle_key, bank_key, option_type_key, creation_date_key, creation_time_key, completion_date_key, completion_time_key
- **Measures**: All financial amounts stored as BIGINT cents (amount_financed_amount, profit_amount, vsc_rev_amount, gap_rev_amount, rpt_amount, etc.)
- **Other**: term, days_to_payment, _source_file_name, _load_timestamp

### Dimension Table Schema
- **Keys**: deal_key, deal_state_key, deal_type_key, source_key, option_type_key, title_registration_option_key, processor_key, tax_processor_key, fee_processor_key, lienholder_key
- **Attributes**: plate_transfer, title_only, buyer_not_lessee, down_payment_status, needs_temporary_registration_tags, source_name, other_source_description
- **Timestamps**: creation_date_utc, completion_date_utc, state_asof_utc
- **Metadata**: _source_table, _load_timestamp

## Data Flow

```
Bronze Tables → Silver Layer → Gold Layer (New)
     ↓
Silver.deal.big_deal → Gold Layer (Existing/gold_big_deal)
```

## Validation and Comparison

The `models/silver/validation_queries.sql` file contains comprehensive queries to:

1. **Record Count Comparison**: Compare deal counts across bronze, silver, and gold layers
2. **Deal Key Overlap Analysis**: Identify deals that exist in some layers but not others
3. **Financial Measures Comparison**: Compare financial calculations across all layers
4. **Summary Statistics**: Aggregate comparisons of key metrics
5. **Schema Documentation**: Document the structure of each layer

## Implementation Benefits

### Silver Layer Approach
- **Data Lineage Transparency**: Clear visibility into bronze table sources
- **Reduced Dependencies**: No reliance on silver.deal.big_deal table
- **Granular Control**: Specific business logic at the source level
- **Performance**: Direct bronze access may improve performance
- **Debugging**: Easier to trace data issues to source

### Schema Consistency
- **Seamless Migration**: New tables can replace existing ones without downstream impact
- **A/B Testing**: Can compare results between approaches
- **Risk Mitigation**: Existing tables remain unchanged during development
- **Business Continuity**: No disruption to current reporting

## Next Steps

1. **Test Silver Layer**: Validate silver tables against bronze sources
2. **Compare Results**: Use validation queries to compare silver vs gold_big_deal
3. **Test Gold Layer**: Validate gold tables against silver sources
4. **Performance Testing**: Compare query performance between approaches
5. **Migration Planning**: Plan transition from gold_big_deal to new gold tables
6. **Documentation**: Update business user documentation for any differences found 