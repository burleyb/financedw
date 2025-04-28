# Step-by-Step Guide for Hex Integration (`finance_dw`)

**Assumptions:**

*   You have an active Hex account and workspace.
*   You have permissions in Hex to create projects, data connections, and apps.
*   The `finance_dw` catalog with its `dimensions`, `facts`, and `mart` schemas exists in Databricks Unity Catalog, and the tables are populated (or will be).
*   A Databricks SQL Warehouse is running and accessible.
*   You have the connection details for Databricks, including:
    *   Workspace URL (e.g., `dbc-xxxxxxxx-xxxx.cloud.databricks.com`)
    *   SQL Warehouse HTTP Path
    *   Authentication credentials (e.g., Service Principal ID and Secret, Personal Access Token, or OAuth details). The identity used must have been granted appropriate permissions in Unity Catalog (see `docs/setup/unity_catalog_setup.sql`).

---

**Steps:**

1.  **Create a New Hex Project:**
    *   Log in to your Hex workspace.
    *   Create a new project. Name it appropriately, for example: `Finance Analytics Dashboard`.

2.  **Configure Databricks Data Connection:**
    *   Within your new Hex project (or via the workspace settings), navigate to **Data Sources** > **Add data connection**.
    *   Select **Databricks** as the data source type.
    *   Fill in the connection details:
        *   **Connection Name:** e.g., `Databricks Finance DW`
        *   **Workspace URL:** Your Databricks workspace URL.
        *   **HTTP Path:** The HTTP Path for your Databricks SQL Warehouse.
        *   **Authentication:** Select your chosen method (e.g., Service Principal, PAT) and provide the necessary credentials.
    *   **Test** the connection to ensure Hex can communicate with your Databricks SQL Warehouse.
    *   **Save** the connection.

3.  **Set Up Shared Project Logic / Input Parameters:**
    *   Navigate to the **Logic** view within your Hex project.
    *   Use **Input Parameters** (found in the right-hand panel or via the "+" button) to create reusable filters based on the examples in `implementation_plan.md`:
        *   **Date Range:** Add a `Date range` input parameter. Name it `date_range`. Set default values if desired. (This will provide `{{ date_range.start }}` and `{{ date_range.end }}` variables).
        *   **Product Types:** Add a `Multiselect` input parameter. Name it `product_types`. Manually add options (e.g., based on values in `dim_product`) or configure it to query distinct values from `finance_dw.dimensions.dim_product`.
        *   **Department:** Add a `Dropdown` or `Multiselect` input parameter named `department`. Populate with relevant departments from `dim_employee`.
        *   *(Add other common filters as needed, e.g., Deal Status based on `dim_deal_status`)*.
    *   These parameters will be available to reference in your SQL cells using the `{{ parameter_name }}` syntax.

4.  **Create Analysis Files/Notebooks:**
    *   Within the Hex project's Logic view, you can create multiple analysis files (notebooks) if desired, perhaps one for "Revenue Analysis" and another for "Commission Analysis", or combine them.

5.  **Implement Key Report SQL Queries:**
    *   In your chosen analysis file, add **SQL cells**.
    *   Select the Databricks data connection (`Databricks Finance DW`) created in Step 2.
    *   Paste or write the SQL queries provided in the `implementation_plan.md`, ensuring you use the correct Unity Catalog path (`finance_dw.facts.<table_name>`, `finance_dw.dimensions.<table_name>`) and reference the Input Parameters created in Step 3.

    *Example (Revenue Analysis Query in a SQL Cell):*
    ```sql
    -- Hex SQL Cell: Revenue Analysis
    SELECT
      d.year_actual, -- Using dim_date column names
      d.month_name, -- Using dim_date column names
      p.product_name, -- Using dim_product column name
      SUM(f.vsc_rev) AS vsc_revenue, -- Using fact_deals column name
      SUM(f.gap_rev) AS gap_revenue, -- Using fact_deals column name
      SUM(f.reserve) AS reserve_revenue, -- Using fact_deals column name
      SUM(f.profit) AS total_profit -- Using fact_deals column name
    FROM finance_dw.facts.fact_deals f
    JOIN finance_dw.dimensions.dim_date d ON f.creation_date_key = d.date_key
    JOIN finance_dw.dimensions.dim_product p ON f.product_key = p.product_key
    WHERE d.date_actual BETWEEN '{{ date_range.start }}' AND '{{ date_range.end }}' -- Referencing date range parameter
      AND p.product_name IN ( {{ product_types }} ) -- Referencing multiselect parameter
    GROUP BY d.year_actual, d.month_name, p.product_name
    ORDER BY d.year_actual, d.month_name, p.product_name;
    ```
    *Add another SQL cell for the Commission Analysis query, similarly referencing `{{ date_range.start }}`, `{{ date_range.end }}`, and `{{ department }}`.*
    *   Run the SQL cells to generate dataframes (e.g., `df_revenue`, `df_commission`).

6.  **Build Visualizations:**
    *   Add **Chart cells** (or Table Display, Map, etc.) to your analysis file.
    *   Select the output dataframe from the relevant SQL cell (e.g., `df_revenue`) as the input data source for the chart.
    *   Configure the charts based on the ideas in the `implementation_plan.md`:
        *   Revenue: Bar chart (Month on X-axis, Revenue on Y-axis, grouped/colored by Product Type).
        *   Commissions: Table Display showing Employee Name, Role, Month, Deals Closed, Commissions, Profit.
        *   *(Create other charts: Line charts for trends, scatter plots, maps based on geography, etc.)*

7.  **Assemble Dashboards/Apps:**
    *   Switch to the **App Builder** view in Hex.
    *   Drag and drop the Input Parameters, SQL cells (optional, can be hidden), and Visualization cells onto the canvas.
    *   Arrange them logically to create an interactive dashboard or data application for end-users.

8.  **Share with Stakeholders:**
    *   Use Hex's **Share** functionality to publish the app and grant view or run access to the intended audience (e.g., the `finance_team_group`).

---

This guide provides the workflow within Hex. Remember to adapt SQL queries and parameter configurations based on the exact column names in your final Databricks tables (e.g., using `dim_driver` instead of `dim_customer`) and the specific filters your finance team requires. 