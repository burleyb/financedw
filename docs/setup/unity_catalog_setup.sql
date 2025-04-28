-- Unity Catalog Setup Guide for Finance DW
-- Execute these commands in a Databricks notebook or SQL Editor.
-- Replace placeholders like 'hex_service_principal_id' and 'finance_team_group'.

-- Assumptions:
-- * You have permissions to create catalogs and schemas.
-- * Unity Catalog is enabled for your workspace.

CREATE CATALOG IF NOT EXISTS finance_gold
COMMENT 'Temp Gold Catalog for the Finance Data Warehouse until ready to move to gold.';

USE CATALOG finance_gold;

CREATE SCHEMA IF NOT EXISTS finance
COMMENT 'Schema for finance dimension and fact tables (e.g., fact_deals, dim_driver, dim_vehicle) for the finance data warehouse.';



-- -- Step 3: Grant necessary permissions (adjust principal/group names)

-- -- Grant usage access to the catalog for Hex and the finance team
-- GRANT USE CATALOG ON CATALOG finance_dw TO `hex_service_principal_id`;
-- GRANT USE CATALOG ON CATALOG finance_dw TO `finance_team_group`;

-- -- Grant usage access to the schemas for Hex and the finance team
-- GRANT USE SCHEMA ON SCHEMA finance_dw.dimensions TO `hex_service_principal_id`;
-- GRANT USE SCHEMA ON SCHEMA finance_dw.facts TO `hex_service_principal_id`;
-- GRANT USE SCHEMA ON SCHEMA finance_dw.mart TO `hex_service_principal_id`;

-- GRANT USE SCHEMA ON SCHEMA finance_dw.dimensions TO `finance_team_group`;
-- GRANT USE SCHEMA ON SCHEMA finance_dw.facts TO `finance_team_group`;
-- GRANT USE SCHEMA ON SCHEMA finance_dw.mart TO `finance_team_group`;

-- -- Grant SELECT (read) access on all current and future tables/views in the schemas
-- -- NOTE: You might want more granular permissions (e.g., only specific tables)
-- GRANT SELECT ON ALL TABLES IN SCHEMA finance_dw.dimensions TO `hex_service_principal_id`;
-- GRANT SELECT ON ALL TABLES IN SCHEMA finance_dw.facts TO `hex_service_principal_id`;
-- GRANT SELECT ON ALL TABLES IN SCHEMA finance_dw.mart TO `hex_service_principal_id`;
-- -- Granting future privileges ensures new tables are automatically accessible
-- -- Optional: Set schema owner if desired (replace with your admin role/group)
-- -- ALTER SCHEMA finance_dw.dimensions SET OWNER TO `<your_admin_role_or_group>`; 
-- -- ALTER SCHEMA finance_dw.facts SET OWNER TO `<your_admin_role_or_group>`;
-- -- ALTER SCHEMA finance_dw.mart SET OWNER TO `<your_admin_role_or_group>`;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA finance_dw.dimensions TO `hex_service_principal_id`;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA finance_dw.facts TO `hex_service_principal_id`;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA finance_dw.mart TO `hex_service_principal_id`;


-- GRANT SELECT ON ALL TABLES IN SCHEMA finance_dw.dimensions TO `finance_team_group`;
-- GRANT SELECT ON ALL TABLES IN SCHEMA finance_dw.facts TO `finance_team_group`;
-- GRANT SELECT ON ALL TABLES IN SCHEMA finance_dw.mart TO `finance_team_group`;
-- -- Granting future privileges ensures new tables are automatically accessible
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA finance_dw.dimensions TO `finance_team_group`;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA finance_dw.facts TO `finance_team_group`;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA finance_dw.mart TO `finance_team_group`;


-- Step 4: Verify Setup (Optional)
/*
SHOW CATALOGS LIKE 'finance_dw';
SHOW SCHEMAS IN finance_dw;
SHOW GRANTS ON CATALOG finance_dw;
SHOW GRANTS ON SCHEMA finance_dw.dimensions;
SHOW GRANTS ON SCHEMA finance_dw.facts;
SHOW GRANTS ON SCHEMA finance_dw.mart;
*/ 