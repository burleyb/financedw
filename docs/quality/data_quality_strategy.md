# Data Quality Monitoring Strategy for Finance DW

This document outlines the strategy for monitoring data quality within the `finance_dw` data warehouse.

## Goals

*   Ensure the accuracy and reliability of financial reporting.
*   Increase trust in the data warehouse among finance users.
*   Identify and resolve data issues proactively.

## Approach

Data quality checks will be implemented primarily using SQL queries run periodically (e.g., daily, after ETL runs). Results can be logged to a monitoring table, or alerts can be configured based on failure thresholds. Consider using Databricks SQL Alerts or integrating with external monitoring tools.

## Key Check Categories

Based on `implementation_plan.mdc`, the following checks are crucial:

1.  **Completeness Checks:**
    *   Verify that critical columns in fact and dimension tables are not NULL where expected (e.g., `deal_id`, `driver_key` in `fact_deals`, `date_key` in `dim_date`).
    *   Monitor row counts over time for unexpected drops or stagnation.
    *   **Example:** `SELECT COUNT(*) FROM finance_dw.facts.fact_deals WHERE driver_key IS NULL;` (Expect 0)

2.  **Accuracy Checks:**
    *   Verify financial calculations (e.g., `profit = revenue - cost`, ensuring consistency across related fields).
    *   Compare key metrics (e.g., total revenue) against source system reports or known benchmarks where possible.
    *   Check for duplicate entries where uniqueness is expected (e.g., primary keys in dimension tables, unique transaction IDs).
    *   **Example:** `SELECT deal_id, SUM(payment_amount) FROM finance_dw.facts.fact_payments GROUP BY deal_id HAVING SUM(payment_amount) > (SELECT amount_financed FROM finance_dw.facts.fact_deals WHERE id = deal_id);` (Identify potential overpayments)

3.  **Validity Checks (Range/Format):**
    *   Ensure dates fall within expected ranges (e.g., no future transaction dates).
    *   Validate formats for fields like email addresses or phone numbers (if applicable).
    *   Check enumerated values against allowed lists (e.g., `deal_state` in `fact_deals` should exist in `dim_deal_status`).
    *   **Example:** `SELECT COUNT(*) FROM finance_dw.dimensions.dim_date WHERE year > YEAR(CURRENT_DATE()) + 1;` (Expect 0)

4.  **Referential Integrity Checks:**
    *   Verify that all foreign keys in fact tables exist as primary keys in their corresponding dimension tables.
    *   This ensures that facts can always be joined correctly to their context.
    *   **Example:** `SELECT COUNT(*) FROM finance_dw.facts.fact_deals f LEFT JOIN finance_dw.dimensions.dim_customer c ON f.driver_key = c.driver_key WHERE c.driver_key IS NULL;` (Expect 0)

## Implementation

*   Develop specific SQL queries for each required check based on business rules.
*   Organize these queries (e.g., in Databricks notebooks or SQL files).
*   Schedule the execution of these checks using Databricks Jobs.
*   Define alerting mechanisms for check failures.
*   Establish a process for investigating and resolving identified data quality issues.

## Future Considerations

*   Explore using dedicated data quality frameworks like Great Expectations or dbt tests for more complex validation rules and automated documentation.
*   Implement data profiling to understand data distributions and identify potential anomalies. 