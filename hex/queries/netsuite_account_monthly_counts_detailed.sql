-- NetSuite Account Monthly Transaction Counts - Detailed Version
-- Query to count entries by month for each account with comprehensive account information
-- Includes account hierarchy, type, and additional metrics

WITH monthly_account_stats AS (
    SELECT 
        a.id AS account_id,
        a.accountsearchdisplayname AS account_display_name,
        a.fullname AS account_full_name,
        a.acctnumber AS account_number,
        a.accttype AS account_type,
        a.parent AS parent_account_id,
        a.issummary AS is_summary_account,
        a.isinactive AS is_inactive,
        YEAR(t.trandate) AS transaction_year,
        MONTH(t.trandate) AS transaction_month,
        DATE_FORMAT(t.trandate, 'yyyy-MM') AS year_month,
        COUNT(tal.transaction) AS entry_count,
        COUNT(DISTINCT tal.transaction) AS unique_transaction_count,
        SUM(COALESCE(tal.amount, 0)) AS total_amount,
        SUM(COALESCE(tal.debit, 0)) AS total_debits,
        SUM(COALESCE(tal.credit, 0)) AS total_credits,
        MIN(t.trandate) AS earliest_transaction_date,
        MAX(t.trandate) AS latest_transaction_date
    FROM bronze.ns.transactionaccountingline tal
    INNER JOIN bronze.ns.transaction t 
        ON tal.transaction = t.id
    INNER JOIN bronze.ns.account a 
        ON tal.account = a.id
    WHERE t.trandate IS NOT NULL
        AND tal._fivetran_deleted = FALSE
        AND t._fivetran_deleted = FALSE
        AND a._fivetran_deleted = FALSE
    GROUP BY 
        a.id,
        a.accountsearchdisplayname,
        a.fullname,
        a.acctnumber,
        a.accttype,
        a.parent,
        a.issummary,
        a.isinactive,
        YEAR(t.trandate),
        MONTH(t.trandate),
        DATE_FORMAT(t.trandate, 'yyyy-MM')
)

SELECT 
    mas.account_id,
    COALESCE(mas.account_display_name, mas.account_full_name, 'Unknown Account') AS account_display_name,
    mas.account_number,
    mas.account_type,
    mas.parent_account_id,
    parent_acct.accountsearchdisplayname AS parent_account_name,
    mas.is_summary_account,
    mas.is_inactive,
    mas.transaction_year,
    mas.transaction_month,
    mas.year_month,
    mas.entry_count,
    mas.unique_transaction_count,
    mas.total_amount,
    mas.total_debits,
    mas.total_credits,
    mas.earliest_transaction_date,
    mas.latest_transaction_date,
    -- Calculate running totals
    SUM(mas.entry_count) OVER (
        PARTITION BY mas.account_id 
        ORDER BY mas.transaction_year, mas.transaction_month 
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_entry_count
FROM monthly_account_stats mas
LEFT JOIN bronze.ns.account parent_acct 
    ON mas.parent_account_id = parent_acct.id
    AND parent_acct._fivetran_deleted = FALSE
ORDER BY 
    mas.account_id,
    mas.transaction_year DESC,
    mas.transaction_month DESC; 