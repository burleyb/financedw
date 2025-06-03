-- Simple NetSuite Account Monthly Entry Counts
-- Count of transaction entries by month for each account with account ID and display name

SELECT 
    a.id AS account_id,
    a.accountsearchdisplayname AS account_display_name,
    DATE_FORMAT(t.trandate, 'yyyy-MM') AS year_month,
    COUNT(*) AS entry_count
FROM bronze.ns.transactionaccountingline tal
INNER JOIN bronze.ns.transaction t ON tal.transaction = t.id
INNER JOIN bronze.ns.account a ON tal.account = a.id
WHERE t.trandate IS NOT NULL
    AND tal._fivetran_deleted = FALSE
    AND t._fivetran_deleted = FALSE
    AND a._fivetran_deleted = FALSE
GROUP BY 
    a.id,
    a.accountsearchdisplayname,
    DATE_FORMAT(t.trandate, 'yyyy-MM')
ORDER BY 
    a.id,
    year_month DESC; 