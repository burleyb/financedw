# Silver vs Gold_Big_Deal: Side-by-Side Structure Comparison

## Overview
This document provides a detailed side-by-side comparison of table structures between the `silver` and `gold_big_deal` layers, highlighting all differences in column definitions, data types, and metadata fields.

---

## Dimension Tables with Structural Differences

### 1. `dim_driver`

| **Silver Structure** | **Gold_Big_Deal Structure** |
|---------------------|---------------------------|
| `driver_key STRING NOT NULL` | `driver_key STRING NOT NULL` |
| `first_name STRING` | `first_name STRING` |
| `middle_name STRING` | `middle_name STRING` |
| `last_name STRING` | `last_name STRING` |
| `name_suffix STRING` | `name_suffix STRING` |
| `email STRING` | `email STRING` |
| `phone_number STRING` | `phone_number STRING` |
| `home_phone_number STRING` | ❌ **MISSING** |
| `city STRING` | `city STRING` |
| `state STRING` | `state STRING` |
| `zip STRING` | `zip STRING` |
| `county STRING` | `county STRING` |
| `dob DATE` | ❌ **MISSING** |
| `marital_status STRING` | ❌ **MISSING** |
| `age INT` | ❌ **MISSING** |
| `finscore DOUBLE` | ❌ **MISSING** |
| `_source_table STRING` | ❌ **MISSING** |
| ❌ **MISSING** | `_source_file_name STRING` |
| `_load_timestamp TIMESTAMP` | `_load_timestamp TIMESTAMP` |

**Impact**: Silver has 6 additional demographic and financial fields that provide richer customer insights.

---

### 2. `dim_bank`

| **Silver Structure** | **Gold_Big_Deal Structure** |
|---------------------|---------------------------|
| `bank_key STRING NOT NULL` | `bank_key STRING NOT NULL` |
| `bank_name STRING` | `bank_name STRING` |
| `bank_id INT` | ❌ **MISSING** |
| `bank_slug STRING` | ❌ **MISSING** |
| ❌ **MISSING** | `address STRING` |
| ❌ **MISSING** | `bank_description STRING` |
| ❌ **MISSING** | `bank_display_name STRING` |
| ❌ **MISSING** | `city STRING` |
| ❌ **MISSING** | `is_auto_structure BOOLEAN` |
| ❌ **MISSING** | `is_auto_structure_buyout BOOLEAN` |
| ❌ **MISSING** | `is_auto_structure_refi BOOLEAN` |
| ❌ **MISSING** | `logo_url STRING` |
| ❌ **MISSING** | `min_credit_score INT` |
| ❌ **MISSING** | `phone STRING` |
| ❌ **MISSING** | `signing_solution STRING` |
| ❌ **MISSING** | `state STRING` |
| ❌ **MISSING** | `zip STRING` |
| `_source_table STRING` | ❌ **MISSING** |
| ❌ **MISSING** | `_source_file_name STRING` |
| `_load_timestamp TIMESTAMP` | `_load_timestamp TIMESTAMP` |

**Impact**: Gold_big_deal has 12 additional bank detail fields that provide comprehensive bank information and capabilities.

---

### 3. `dim_vehicle`

| **Silver Structure** | **Gold_Big_Deal Structure** |
|---------------------|---------------------------|
| `vehicle_key STRING NOT NULL` | `vehicle_key STRING NOT NULL` |
| `vin STRING` | `vin STRING` |
| `make STRING` | `make STRING` |
| `model STRING` | `model STRING` |
| `model_year INT` | `model_year STRING` ⚠️ **TYPE DIFF** |
| `color STRING` | `color STRING` |
| `vehicle_type STRING` | `vehicle_type STRING` |
| `fuel_type STRING` | `fuel_type STRING` |
| `kbb_trim_name STRING` | `kbb_trim_name STRING` |
| `mileage INT` | ❌ **MISSING** |
| `odometer_status STRING` | ❌ **MISSING** |
| `jdp_retail_value DOUBLE` | ❌ **MISSING** |
| `jdp_trade_in_value DOUBLE` | ❌ **MISSING** |
| `jdp_valuation_date DATE` | ❌ **MISSING** |
| `kbb_retail_value DOUBLE` | ❌ **MISSING** |
| `kbb_trade_in_value DOUBLE` | ❌ **MISSING** |
| `kbb_valuation_date DATE` | ❌ **MISSING** |
| `_source_table STRING` | ❌ **MISSING** |
| ❌ **MISSING** | `_source_file_name STRING` |
| `_load_timestamp TIMESTAMP` | `_load_timestamp TIMESTAMP` |

**Impact**: Silver has 8 additional vehicle valuation and condition fields. Also has data type difference for `model_year`.

---

### 4. `dim_lienholder`

| **Silver Structure** | **Gold_Big_Deal Structure** |
|---------------------|---------------------------|
| `lienholder_key STRING NOT NULL` | `lienholder_key STRING NOT NULL` |
| `lienholder_name STRING` | `lienholder_name STRING` |
| `lienholder_slug STRING` | ❌ **MISSING** |
| `_source_table STRING` | ❌ **MISSING** |
| ❌ **MISSING** | `_source_file_name STRING` |
| `_load_timestamp TIMESTAMP` | `_load_timestamp TIMESTAMP` |

**Impact**: Silver has additional slug field for URL-friendly identifiers.

---

### 5. `dim_processor`

| **Silver Structure** | **Gold_Big_Deal Structure** |
|---------------------|---------------------------|
| `processor_key STRING NOT NULL` | `processor_key STRING NOT NULL` |
| `processor_name STRING` | `processor_name STRING` |
| `processor_email STRING` | `processor_email STRING` |
| `processor_type STRING` | ❌ **MISSING** |
| `_source_table STRING` | ❌ **MISSING** |
| ❌ **MISSING** | `_source_file_name STRING` |
| `_load_timestamp TIMESTAMP` | `_load_timestamp TIMESTAMP` |

**Impact**: Silver has additional processor type classification.

---

## Fact Tables with Structural Differences

### 1. `fact_deals`

| **Silver Structure** | **Gold_Big_Deal Structure** |
|---------------------|---------------------------|
| `deal_key STRING NOT NULL` | `deal_key STRING NOT NULL` |
| `deal_state_key STRING` | `deal_state_key STRING` |
| `deal_type_key STRING` | `deal_type_key STRING` |
| `driver_key STRING` | `driver_key STRING` |
| `vehicle_key STRING` | `vehicle_key STRING` |
| `bank_key STRING` | `bank_key STRING` |
| `option_type_key STRING` | `option_type_key STRING` |
| `creation_date_key INT` | `creation_date_key INT` |
| `creation_time_key INT` | `creation_time_key INT` |
| `completion_date_key INT` | `completion_date_key INT` |
| `completion_time_key INT` | `completion_time_key INT` |
| `amount_financed_amount BIGINT` | `amount_financed_amount BIGINT` |
| `payment_amount BIGINT` | `payment_amount BIGINT` |
| `money_down_amount BIGINT` | `money_down_amount BIGINT` |
| `sell_rate_amount BIGINT` | `sell_rate_amount BIGINT` |
| `buy_rate_amount BIGINT` | `buy_rate_amount BIGINT` |
| `profit_amount BIGINT` | `profit_amount BIGINT` |
| `vsc_price_amount BIGINT` | `vsc_price_amount BIGINT` |
| `vsc_cost_amount BIGINT` | `vsc_cost_amount BIGINT` |
| `vsc_rev_amount BIGINT` | `vsc_rev_amount BIGINT` |
| `gap_price_amount BIGINT` | `gap_price_amount BIGINT` |
| `gap_cost_amount BIGINT` | `gap_cost_amount BIGINT` |
| `gap_rev_amount BIGINT` | `gap_rev_amount BIGINT` |
| `total_fee_amount BIGINT` | `total_fee_amount BIGINT` |
| `doc_fee_amount BIGINT` | `doc_fee_amount BIGINT` |
| `bank_fees_amount BIGINT` | `bank_fees_amount BIGINT` |
| `registration_transfer_fee_amount BIGINT` | `registration_transfer_fee_amount BIGINT` |
| `title_fee_amount BIGINT` | `title_fee_amount BIGINT` |
| `new_registration_fee_amount BIGINT` | `new_registration_fee_amount BIGINT` |
| `reserve_amount BIGINT` | `reserve_amount BIGINT` |
| `base_tax_amount BIGINT` | `base_tax_amount BIGINT` |
| `warranty_tax_amount BIGINT` | `warranty_tax_amount BIGINT` |
| `rpt_amount BIGINT` | `rpt_amount BIGINT` |
| `ally_fees_amount BIGINT` | ❌ **MISSING** |
| `term INT` | `term INT` |
| `days_to_payment INT` | `days_to_payment INT` |
| `_source_table STRING` | ❌ **MISSING** |
| ❌ **MISSING** | `_source_file_name STRING` |
| `_load_timestamp TIMESTAMP` | `_load_timestamp TIMESTAMP` |

**Impact**: Silver has additional ally fees tracking. Metadata field naming differs.

---

### 2. `fact_deal_netsuite`

| **Silver Structure** | **Gold_Big_Deal Structure** |
|---------------------|---------------------------|
| `deal_key STRING NOT NULL` | `deal_key STRING NOT NULL` |
| `creation_date_key INT` | `creation_date_key INT` |
| `total_revenue BIGINT` | ❌ **MISSING** |
| `total_costs BIGINT` | ❌ **MISSING** |
| ❌ **MISSING** | `bank_buyout_fees_5520 BIGINT` |
| ❌ **MISSING** | `commission_5302 BIGINT` |
| ❌ **MISSING** | `customer_experience_5403 BIGINT` |
| ❌ **MISSING** | `direct_emp_benefits_5330 BIGINT` |
| ❌ **MISSING** | `direct_payroll_tax_5340 BIGINT` |
| ❌ **MISSING** | `doc_fees_chargeback_rev_4130c BIGINT` |
| ❌ **MISSING** | `doc_fees_rev_4130 BIGINT` |
| ❌ **MISSING** | `funding_clerks_5301 BIGINT` |
| ❌ **MISSING** | `gap_advance_5120a BIGINT` |
| ❌ **MISSING** | `gap_advance_rev_4120a BIGINT` |
| ❌ **MISSING** | `gap_chargeback_rev_4121 BIGINT` |
| ❌ **MISSING** | `gap_cor_5120 BIGINT` |
| ❌ **MISSING** | `gap_cost_rev_4120c BIGINT` |
| ❌ **MISSING** | `gap_rev_4120 BIGINT` |
| ❌ **MISSING** | `gap_volume_bonus_rev_4120b BIGINT` |
| ❌ **MISSING** | `ic_payoff_team_5304 BIGINT` |
| ❌ **MISSING** | `outbound_commission_5305 BIGINT` |
| ❌ **MISSING** | `payoff_variance_5400 BIGINT` |
| ❌ **MISSING** | `penalties_5404 BIGINT` |
| ❌ **MISSING** | `postage_5510 BIGINT` |
| ❌ **MISSING** | `registration_variance_5402 BIGINT` |
| ❌ **MISSING** | `repo BOOLEAN` |
| ❌ **MISSING** | `reserve_bonus_rev_4106 BIGINT` |
| ❌ **MISSING** | `reserve_chargeback_rev_4107 BIGINT` |
| ❌ **MISSING** | `rev_reserve_4105 BIGINT` |
| ❌ **MISSING** | `sales_guarantee_5303 BIGINT` |
| ❌ **MISSING** | `sales_tax_variance_5401 BIGINT` |
| ❌ **MISSING** | `title_clerks_5320 BIGINT` |
| ❌ **MISSING** | `titling_fees_rev_4141 BIGINT` |
| ❌ **MISSING** | `vsc_advance_5110a BIGINT` |
| ❌ **MISSING** | `vsc_advance_rev_4110a BIGINT` |
| ❌ **MISSING** | `vsc_chargeback_rev_4111 BIGINT` |
| ❌ **MISSING** | `vsc_cor_5110 BIGINT` |
| ❌ **MISSING** | `vsc_cost_rev_4110c BIGINT` |
| ❌ **MISSING** | `vsc_rev_4110 BIGINT` |
| ❌ **MISSING** | `vsc_volume_bonus_rev_4110b BIGINT` |
| `_source_table STRING` | ❌ **MISSING** |
| ❌ **MISSING** | `_source_file_name STRING` |
| `_load_timestamp TIMESTAMP` | `_load_timestamp TIMESTAMP` |

**Impact**: Completely different approaches - Silver uses aggregated totals (2 fields) while Gold_big_deal has granular NetSuite account mapping (25+ fields).

---

## Tables with Identical Structures ✅

The following tables have matching structures between silver and gold_big_deal:

- `dim_deal`
- `dim_deal_state`
- `dim_deal_type`
- `dim_down_payment_status` (now fixed)
- `dim_employee`
- `dim_employee_pod`
- `dim_employee_titling_pod`
- `dim_employment`
- `dim_employment_status`
- `dim_geography`
- `dim_marital_status`
- `dim_odometer_status`
- `dim_option_type`
- `dim_pay_frequency`
- `dim_pod`
- `dim_source`
- `dim_title_registration_option`
- `dim_titling_pod`
- `dim_vsc_type`
- `fact_deal_commissions`
- `fact_deal_payoff`

---

## Summary of Key Differences

### Metadata Patterns
- **Silver**: Uses `_source_table STRING` to track bronze table sources
- **Gold_big_deal**: Uses `_source_file_name STRING` to track file sources

### Data Richness
- **Silver**: More comprehensive in driver demographics, vehicle valuations
- **Gold_big_deal**: More comprehensive in bank details, NetSuite account granularity

### Data Types
- **model_year**: Silver uses `INT`, Gold_big_deal uses `STRING`

### Architectural Approach
- **Silver**: Reads from bronze tables with enhanced data joins
- **Gold_big_deal**: Reads from `silver.deal.big_deal` flat table

---

## Recommendations

1. **Standardize metadata**: Choose either `_source_table` or `_source_file_name` consistently
2. **Merge best features**: Combine driver demographics from silver with bank details from gold_big_deal
3. **Decide on NetSuite approach**: Aggregated vs granular account mapping
4. **Standardize data types**: Align `model_year` and other type differences
5. **Update gold to read from silver**: Follow medallion architecture pattern 