# Finance Data Warehouse - Business User Training Outline

This document outlines the structure and content for training finance users on the new `finance_dw` data warehouse and Hex reporting environment.

## Target Audience

*   Finance team members involved in reporting, analysis, and data exploration.

## Training Goals

*   Understand the structure and purpose of the star schema data warehouse.
*   Learn how to connect to and query the data warehouse using Hex.
*   Become proficient in using pre-built Hex dashboards and reports.
*   Develop skills for creating basic ad-hoc reports and visualizations in Hex.
*   Understand data governance, PII handling, and where to find documentation.

## Training Modules

**Module 1: Introduction to the Finance Data Warehouse**
*   Overview of the project goals and benefits.
*   Explanation of the star schema concept (facts vs. dimensions).
*   Key business processes covered (deals, payments, commissions).
*   Overview of the main fact and dimension tables (`fact_deals`, `dim_driver`, `dim_vehicle`, `dim_date`, etc.).
*   Data dictionary location and usage (`docs/data-dictionary/`).
*   Data refresh frequency and latency expectations.

**Module 2: Introduction to Hex**
*   Overview of the Hex interface (Projects, Logic View, App Builder).
*   Connecting to the `Databricks Finance DW` data source.
*   Understanding Hex SQL cells and Python cells (basic usage).
*   Using Input Parameters (Date Range, Dropdowns, Multiselect).

**Module 3: Using Pre-Built Dashboards & Reports**
*   Navigating the primary `Finance Analytics Dashboard` Hex app.
*   Understanding the key reports (Revenue, Commission, Funnel Analysis, etc.).
*   Using filters (Input Parameters) effectively.
*   Exporting data and visuals from Hex.
*   Understanding scheduled reports (if configured).

**Module 4: Ad-Hoc Reporting in Hex**
*   Creating a new Hex project/notebook.
*   Writing basic SQL queries against `finance_dw` tables (using `SELECT`, `JOIN`, `WHERE`, `GROUP BY`).
*   Referencing the data dictionary for table/column information.
*   Creating simple visualizations (Tables, Bar Charts, Line Charts) from query results.
*   Basic use of the App Builder to combine elements.
*   Saving and sharing simple analyses.
*   **Example Queries:** Provide documented, runnable examples for common tasks (e.g., finding deals in a specific state, calculating average profit per product).

**Module 5: Data Governance & Best Practices**
*   Importance of data security and PII handling (referencing specific PII protocols).
*   Data quality process overview and how to report potential issues.
*   Hex project naming conventions and organization.
*   Where to find help (documentation, office hours, support contacts).

## Training Delivery Methods

*   Instructor-led sessions (virtual or in-person).
*   Recorded tutorial videos for key tasks (e.g., using filters, creating a basic chart).
*   Hands-on exercises within Hex.
*   Comprehensive user guide documentation (`docs/user-guides/`).
*   Regularly scheduled office hours for Q&A and support.

## Next Steps

*   Develop detailed content for each module.
*   Create hands-on exercises and sample data scenarios.
*   Record tutorial videos.
*   Schedule and deliver training sessions. 