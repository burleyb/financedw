-- NetSuite Account Monthly Transaction Counts
-- Query to count entries by month for each account with account ID and display name

SELECT 
    a.id AS account_id,
    a.accountsearchdisplayname AS account_display_name,
    YEAR(t.trandate) AS transaction_year,
    MONTH(t.trandate) AS transaction_month,
    DATE_FORMAT(t.trandate, 'yyyy-MM') AS year_month,
    COUNT(tal.transaction) AS entry_count
FROM bronze.ns.transactionaccountingline tal
INNER JOIN bronze.ns.transaction t 
    ON tal.transaction = t.id
INNER JOIN bronze.ns.account a 
    ON tal.account = a.id
WHERE t.trandate IS NOT NULL
    AND a.accountsearchdisplayname IS NOT NULL
    AND tal._fivetran_deleted = FALSE
    AND t._fivetran_deleted = FALSE
    AND a._fivetran_deleted = FALSE
GROUP BY 
    a.id,
    a.accountsearchdisplayname,
    YEAR(t.trandate),
    MONTH(t.trandate),
    DATE_FORMAT(t.trandate, 'yyyy-MM')
ORDER BY 
    a.id,
    transaction_year DESC,
    transaction_month DESC; 