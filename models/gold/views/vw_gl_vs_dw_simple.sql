-- models/gold/views/vw_gl_vs_dw_simple.sql
-- Simple reconciliation: Silver fact table vs NetSuite posted GL
-- This catches ALL processing issues including DIRECT method problems

CREATE OR REPLACE VIEW gold.finance.vw_gl_vs_dw_simple AS
-- Fixed view to join only on account_number, not account_name to avoid duplicates
WITH direct_method_accounts AS (
  SELECT account_number FROM VALUES
    ('4110A'), -- VSC ADVANCE - ADDED to fix reconciliation issues with advance accounts
    ('4110B'), -- VSC VOLUME BONUS - ADDED to fix reconciliation issues with volume bonus accounts
    ('4120A'), -- GAP ADVANCE - ADDED to fix reconciliation issues with advance accounts
    ('4120B'), -- GAP VOLUME BONUS - ADDED to fix reconciliation issues with volume bonus accounts
    ('5100'), -- COR - LEASE BUYOUT - TYPE B FIX: Major GENJRNL variances
    ('5121'), -- COR - GAP CHARGEBACK - TYPE B FIX: Unallocated variances
    ('5138'), -- COR - PENALTIES AND LATE FEES - TYPE B FIX: Operational transactions
    ('5139'), -- COR - REGISTRATION - TYPE B FIX: Non-posted transaction issues
    ('5140'), -- COR - SALES TAX - TYPE B FIX: Non-posted transaction issues  
    ('5141'), -- COR - TITLE ONLY FEES - TYPE B FIX: Non-posted transaction issues
    ('5210'), -- COR - ACQUISITION - TYPE B FIX: MAJOR $400K+/month GENJRNL variances
    ('5213'), -- TRANSPORTATION - TYPE B FIX: Small operational variances
    ('5301'), -- IC FUNDING CLERKS
    ('5304'), -- IC PAYOFF TEAM  
    ('5320'), -- IC TITLE CLERKS
    ('5330'), -- EMPLOYEE BENEFITS
    ('5340'), -- PAYROLL TAX
    ('5400'), -- COR - PAYOFF EXPENSE - ADDED to capture GENJRNL/BILLCRED/CC expenses without deal_id
    ('5401'), -- COR - SALES TAX EXPENSE - ADDED to capture GENJRNL expenses without deal_id
    ('5402'), -- COR - REGISTRATION EXPENSE - ADDED to capture GENJRNL/BILLCRED/CC expenses without deal_id
    ('5403'), -- COR - CUSTOMER EXPERIENCE - ADDED to capture CC/BILLCRED/GENJRNL expenses without deal_id
    ('5404'), -- COR - PENALTIES - ADDED to capture GENJRNL expenses without deal_id
    ('5510'), -- POSTAGE
    ('5520'), -- BANK BUYOUT FEES
    ('5530'), -- TITLE COR
    ('6000'), -- MANAGER COMP - TYPE B FIX: Major payroll variances from GENJRNL+PAY CHK
    ('6010'), -- COMMISSION - TYPE B FIX: Major commission variances from GENJRNL+PAY CHK
    ('6011'), -- SALESPERSON GUARANTEE - ADDED to fix reconciliation issues
    ('6012'), -- FUNDING CLERK COMP - TYPE B FIX: Compensation variances from payroll
    ('6030'), -- HOURLY - TYPE B FIX: Major payroll variances from GENJRNL+PAY CHK
    ('6040'), -- DEVELOPER COMP - TYPE B FIX: Major compensation variances from payroll
    ('6041'), -- IC DEVELOPER COMP - ADDED to fix reconciliation issues
    ('6100'), -- EMP BENEFITS - TYPE B FIX: Benefits variances from GENJRNL+BILL  
    ('6101'), -- EMP ENGAGEMENT - ADDED to fix reconciliation issues
    ('6110'), -- SALARIES
    ('6120'), -- COMMISSIONS
    ('6130'), -- BONUSES
    ('6140'), -- EMPLOYEE BENEFITS
    ('6150'), -- PAYROLL TAX
    ('6200'), -- PAYROLL TAX - TYPE B FIX: Major tax variances from GENJRNL+PAY CHK
    ('6210'), -- CONTRACTOR FEES
    ('6310'), -- RENT
    ('6320'), -- UTILITIES
    ('6330'), -- INSURANCE
    ('6340'), -- OFFICE SUPPLIES
    ('6350'), -- TELEPHONE
    ('6410'), -- PROFESSIONAL FEES
    ('6420'), -- LEGAL FEES
    ('6510'), -- ADVERTISING
    ('6520'), -- MARKETING
    ('6530'), -- DIGITAL MARKETING - ADDED to fix reconciliation issues
    ('6540'), -- SMS MARKETING - ADDED to fix reconciliation issues
    ('6560'), -- PROMOTIONAL MARKETING - ADDED to fix reconciliation issues
    ('6570'), -- AFFILIATE MARKETING - ADDED to fix reconciliation issues
    ('7001'), -- PAYOFF VARIANCE - TYPE B FIX: Operational adjustment transactions
    ('7004'), -- SALES TAX VARIANCE - TYPE B FIX: Tax variance adjustments
    ('7005'), -- REGISTRATION VARIANCE - TYPE B FIX: Registration variance adjustments
    ('7007'), -- PENALTIES - TYPE B FIX: Penalty transactions
    ('7100'), -- INSURANCE - ADDED to fix reconciliation issues
    ('7110'), -- OFFICE SUPPLIES - ADDED to fix reconciliation issues
    ('7120'), -- POSTAGE - ADDED to fix reconciliation issues
    ('7125'), -- RECRUITING EXPENSE - ADDED to fix reconciliation issues
    ('7130'), -- PROFESSIONAL FEES - ADDED to fix reconciliation issues
    ('7131'), -- BUSINESS LICENSE AND REGISTRATIONS - ADDED to fix reconciliation issues
    ('7140'), -- SUBSCRIPTION FEES - ADDED to fix reconciliation issues
    ('7141'), -- BANK FEES (account 450)
    ('7160'), -- TRAVEL & ENTERTAINMENT - ADDED to capture individual transactions instead of allocation
    ('7170'), -- RENT & LEASE - ADDED to capture individual transactions instead of allocation
    ('7170A'), -- RENT AMORTIZATION - ADDED to fix reconciliation issues
    ('7171'), -- UTILITIES - ADDED to fix reconciliation issues
    ('7172'), -- BUILDING MAINTENANCE & EXPENSE - ADDED to fix reconciliation issues
    ('7174'), -- VEHICLE MAINTENANCE - ADDED to capture individual transactions instead of allocation
    ('7175'), -- DEPRECIATION EXPENSE - ADDED to fix reconciliation issues
    ('7176'), -- AMORTIZATION EXPENSE - ADDED to fix reconciliation issues
    ('7180'), -- INTERNET & TELEPHONE - ADDED to fix reconciliation issues
    ('7190'), -- SALES EXPENSE - ADDED to fix reconciliation issues
    ('7192'), -- ADDED to fix reconciliation issues
    ('8010'), -- STATE INCOME TAX - ADDED to fix reconciliation issues
    ('8011')  -- FEDERAL INCOME TAX - ADDED to fix reconciliation issues
  AS t(account_number)
),
dw_totals AS (
    SELECT
        COALESCE(da.account_number, a.acctnumber) AS account_number,
        MAX(COALESCE(da.account_name, a.fullname)) AS account_name,
        dd.year,
        dd.month,
        -- Negate revenue accounts (4000-4999) to match NetSuite's sign convention
        SUM(CASE 
            WHEN CAST(REGEXP_EXTRACT(COALESCE(da.account_number, a.acctnumber), '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 4999 
            THEN -f.amount_dollars  -- Negate revenue to match NetSuite
            ELSE f.amount_dollars  -- Keep other accounts as is
        END) AS dw_total,
        COUNT(*) as dw_transaction_count,
        -- Add flag to identify DIRECT method accounts
        MAX(CASE WHEN dma.account_number IS NOT NULL THEN TRUE ELSE FALSE END) as is_direct_method_account
    FROM gold.finance.fact_deal_netsuite_transactions f
    LEFT JOIN gold.finance.dim_account da ON f.account_key = da.account_key
    LEFT JOIN bronze.ns.account a ON CAST(f.account_key AS BIGINT) = a.id
    LEFT JOIN gold.finance.dim_date dd ON f.netsuite_posting_date_key = dd.date_key
    LEFT JOIN direct_method_accounts dma ON COALESCE(da.account_number, a.acctnumber) = dma.account_number
    WHERE CAST(REGEXP_EXTRACT(COALESCE(da.account_number, a.acctnumber), '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
    GROUP BY COALESCE(da.account_number, a.acctnumber), dd.year, dd.month
),
ns_posted AS (
    SELECT
        a.acctnumber AS account_number,
        MAX(a.fullname) AS account_name,
        YEAR(t.trandate) AS year,
        MONTH(t.trandate) AS month,
        SUM(tal.amount) AS ns_posted_total,  -- Keep NetSuite's original sign
        COUNT(*) as ns_transaction_count,
        -- Add flag to identify DIRECT method accounts
        MAX(CASE WHEN dma.account_number IS NOT NULL THEN TRUE ELSE FALSE END) as is_direct_method_account
    FROM bronze.ns.transactionaccountingline tal
    JOIN bronze.ns.transaction t ON tal.transaction = t.id
    JOIN bronze.ns.account a ON tal.account = a.id
    LEFT JOIN direct_method_accounts dma ON a.acctnumber = dma.account_number
    WHERE tal.posting = 'T'  -- Always use only posted transactions for reconciliation accuracy
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND t.abbrevtype IN ('SALESORD','CREDITMEMO','CREDMEM','INV','GENJRNL','BILL','BILLCRED','CC','CHK','INV WKST')  -- Ensure all transaction types are included
      AND tal.amount != 0  -- CRITICAL FIX: Exclude zero-amount transactions that inflate counts without financial impact
      AND CAST(REGEXP_EXTRACT(a.acctnumber, '^[0-9]+', 0) AS INT) BETWEEN 4000 AND 8999
    GROUP BY a.acctnumber, YEAR(t.trandate), MONTH(t.trandate)
),
-- For DIRECT method accounts, get total NetSuite amounts (posted + unposted) for comparison
ns_total_direct AS (
    -- Use posted GL (transactionaccountingline) for DIRECT method accounts to match GL exactly
    SELECT
        a.acctnumber AS account_number,
        MAX(a.fullname) AS account_name,
        YEAR(t.trandate) AS year,
        MONTH(t.trandate) AS month,
        SUM(tal.amount) AS ns_total_amount,  -- Keep NetSuite's original sign
        COUNT(*) as ns_total_transaction_count
    FROM bronze.ns.transactionaccountingline tal
    JOIN bronze.ns.transaction t ON tal.transaction = t.id
    JOIN bronze.ns.account a ON tal.account = a.id
    JOIN direct_method_accounts dma ON a.acctnumber = dma.account_number
    WHERE tal.posting = 'T'  -- Only posted GL entries
      AND (t._fivetran_deleted = FALSE OR t._fivetran_deleted IS NULL)
      AND (tal._fivetran_deleted = FALSE OR tal._fivetran_deleted IS NULL)
      AND tal.amount != 0  -- Exclude zero-amount transactions
    GROUP BY a.acctnumber, YEAR(t.trandate), MONTH(t.trandate)
),
-- First calculate aggregated values
aggregated_results AS (
    -- Regular accounts from DW
    SELECT 
        dw.account_number,
        dw.account_name,
        dw.year,
        dw.month,
        dw.dw_total,
        dw.dw_transaction_count,
        ns.ns_posted_total,
        ns.ns_transaction_count
    FROM dw_totals dw
    LEFT JOIN ns_posted ns
      ON dw.account_number = ns.account_number
      AND dw.year = ns.year
      AND dw.month = ns.month
    WHERE COALESCE(dw.is_direct_method_account, FALSE) = FALSE
    
    UNION ALL
    
    -- Direct method accounts from DW
    SELECT 
        dw.account_number,
        dw.account_name,
        dw.year,
        dw.month,
        dw.dw_total,
        dw.dw_transaction_count,
        COALESCE(ns_direct.ns_total_amount, 0) as ns_posted_total,
        COALESCE(ns_direct.ns_total_transaction_count, 0) as ns_transaction_count
    FROM dw_totals dw
    LEFT JOIN ns_total_direct ns_direct
      ON dw.account_number = ns_direct.account_number
      AND dw.year = ns_direct.year
      AND dw.month = ns_direct.month
    WHERE COALESCE(dw.is_direct_method_account, FALSE) = TRUE
    
    UNION ALL
    
    -- Regular accounts from NS not in DW
    SELECT 
        ns.account_number,
        ns.account_name,
        ns.year,
        ns.month,
        0 as dw_total,
        0 as dw_transaction_count,
        ns.ns_posted_total,
        ns.ns_transaction_count
    FROM ns_posted ns
    LEFT JOIN dw_totals dw
      ON ns.account_number = dw.account_number
      AND ns.year = dw.year
      AND ns.month = dw.month
    WHERE dw.account_number IS NULL
    
    UNION ALL
    
    -- Direct method accounts from NS not in DW
    SELECT 
        ns_direct.account_number,
        ns_direct.account_name,
        ns_direct.year,
        ns_direct.month,
        0 as dw_total,
        0 as dw_transaction_count,
        ns_direct.ns_total_amount as ns_posted_total,
        ns_direct.ns_total_transaction_count as ns_transaction_count
    FROM ns_total_direct ns_direct
    LEFT JOIN dw_totals dw
      ON ns_direct.account_number = dw.account_number
      AND ns_direct.year = dw.year
      AND ns_direct.month = dw.month
    WHERE dw.account_number IS NULL
),
-- Deduplicate and aggregate
final_aggregated AS (
    SELECT
        account_number,
        MAX(account_name) AS account_name,
        year,
        month,
        MAX(dw_total) AS dw_total,
        MAX(ns_posted_total) AS ns_posted_total,
        MAX(dw_transaction_count) AS dw_transaction_count,
        MAX(ns_transaction_count) AS ns_transaction_count
    FROM aggregated_results
    GROUP BY account_number, year, month
)

-- Final query with calculated columns
SELECT
    account_number,
    account_name,
    year,
    month,
    dw_total,
    ns_posted_total,
    dw_total - ns_posted_total AS difference,
    dw_transaction_count,
    ns_transaction_count,
    CASE 
        WHEN ABS(dw_total - ns_posted_total) < 0.01 THEN 'MATCH'
        WHEN dw_total = 0 THEN 'MISSING_IN_DW'
        WHEN ns_posted_total = 0 THEN 'MISSING_IN_NS'
        ELSE 'MISMATCH'
    END AS status
FROM final_aggregated
ORDER BY ABS(dw_total - ns_posted_total) DESC;
