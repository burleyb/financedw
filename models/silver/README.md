# Silver Layer Data Models

This directory contains the silver layer data models that read from bronze sources and implement the star schema for the finance data warehouse.

## Overview

The silver layer serves as the cleansed and transformed layer that:
- Reads directly from bronze tables (bronze.leaseend_db_public.*)
- Implements proper data types and validation
- Handles deduplication and data quality
- Creates the foundation for the gold layer star schema
- Uses consistent patterns for incremental updates

## Architecture

All silver tables follow these patterns:
- **Schema**: `silver.finance`
- **Storage**: Delta Lake format with auto-optimization
- **Updates**: MERGE statements for incremental processing
- **Metadata**: `_source_table` and `_load_timestamp` columns
- **Partitioning**: Strategic partitioning for query performance
- **Data Quality**: Date validation, NULL handling, and "Unknown" records

## Dimension Tables

### Core Dimensions

| Table | Description | Source | Key Features |
|-------|-------------|--------|--------------|
| `dim_driver` | Customer/driver information | customers, addresses, contact_db | PII handling, age calculation, finscore |
| `dim_vehicle` | Vehicle details and valuations | cars | VIN-based, KBB/JDP valuations |
| `dim_employee` | Employee information | users | Location partitioning |
| `dim_bank` | Bank/lender information | banks | Active status tracking |
| `dim_deal` | Deal header information | deals | SCD Type 2 for deal progression |

### Lookup Dimensions

| Table | Description | Source | Key Features |
|-------|-------------|--------|--------------|
| `dim_date` | Date dimension | Generated | Fiscal year support (Oct start) |
| `dim_deal_state` | Deal status values | deals | Business logic flags |
| `dim_source` | Lead source types | deals | Marketing channel mapping |
| `dim_option_type` | Product combinations | financial_infos | VSC/GAP flags |
| `dim_geography` | Geographic combinations | addresses, customers | Composite key |
| `dim_employment` | Employment history | employments | SCD Type 2 |

## Fact Tables

### Primary Facts

| Table | Description | Source | Key Features |
|-------|-------------|--------|--------------|
| `fact_deals` | Main deal transactions | deals, cars, financial_infos | All financial measures |
| `fact_deal_commissions` | Commission tracking | deals, financial_infos | Split commission logic |
| `fact_deal_payoff` | Payoff transactions | deals | Lienholder tracking |
| `fact_deal_netsuite` | NetSuite financials | TBD | Placeholder for NS integration |

## Data Patterns

### Monetary Values
- All monetary amounts stored as BIGINT cents (multiply by 100)
- Example: $123.45 stored as 12345

### Date/Time Keys
- Date keys: YYYYMMDD format as INT (e.g., 20231215)
- Time keys: HHMMSS format as INT (e.g., 143022)

### Unknown Records
- All dimensions include "Unknown" records for NULL handling
- Consistent key values: 'unknown', 'Unknown', etc.

### SCD Implementation
- **Type 1**: Most dimensions (overwrite changes)
- **Type 2**: dim_employment, dim_deal (track history)

## Data Quality

### Validation Rules
- Date range validation: 1900-01-01 to 2037-12-31
- Required field validation with COALESCE defaults
- Deduplication using ROW_NUMBER() with updated_at DESC
- Fivetran deletion flag filtering

### Partitioning Strategy
- `dim_driver`: Partitioned by state
- `dim_vehicle`: Partitioned by make
- `dim_employee`: Partitioned by location
- `dim_geography`: Partitioned by state
- `fact_deals`: Partitioned by creation_date_key
- `fact_deal_*`: Partitioned by relevant date_key

## Usage Examples

### Basic Query Pattern
```sql
SELECT 
  d.deal_key,
  dr.first_name,
  dr.last_name,
  v.make,
  v.model,
  fd.amount_financed_amount / 100.0 as amount_financed
FROM silver.finance.fact_deals fd
JOIN silver.finance.dim_driver dr ON fd.driver_key = dr.driver_key
JOIN silver.finance.dim_vehicle v ON fd.vehicle_key = v.vehicle_key
WHERE fd.creation_date_key >= 20231201
```

### Commission Analysis
```sql
SELECT 
  e.name as employee_name,
  SUM(fc.closer_commission_amount) / 100.0 as total_commission
FROM silver.finance.fact_deal_commissions fc
JOIN silver.finance.dim_employee e ON fc.employee_closer_key = e.employee_key
WHERE fc.commission_date_key >= 20231201
GROUP BY e.name
```

## Next Steps

1. **Gold Layer Creation**: Create gold layer tables that read from silver
2. **NetSuite Integration**: Complete fact_deal_netsuite implementation
3. **Additional Dimensions**: Add remaining dimensions as needed
4. **Data Validation**: Implement comprehensive data quality tests
5. **Performance Optimization**: Add Z-ORDER and optimize partitioning

## Maintenance

- Tables should be refreshed incrementally using the MERGE patterns
- Monitor partition sizes and adjust strategies as needed
- Validate data quality after each refresh
- Update "Unknown" record handling as business rules evolve 