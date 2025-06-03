-- VINs by Revenue Recognition Period
-- Query to list all VINs grouped by their revenue recognition period
-- Revenue recognition is based on the first 'signed' state date for each deal

WITH revenue_recognition_data AS (
    SELECT
        ds.deal_id,
        ds.updated_date_utc as revenue_recognition_date_utc,
        ROW_NUMBER() OVER (PARTITION BY ds.deal_id ORDER BY ds.updated_date_utc ASC) as rn
    FROM bronze.leaseend_db_public.deal_states ds
    WHERE ds.state = 'signed' 
        AND ds.deal_id IS NOT NULL 
        AND (ds._fivetran_deleted = FALSE OR ds._fivetran_deleted IS NULL)
),

-- Get the first revenue recognition date per deal
first_revenue_recognition AS (
    SELECT 
        deal_id,
        revenue_recognition_date_utc,
        DATE_FORMAT(revenue_recognition_date_utc, 'yyyy-MM') as revenue_recognition_period,
        YEAR(revenue_recognition_date_utc) as revenue_recognition_year,
        MONTH(revenue_recognition_date_utc) as revenue_recognition_month,
        QUARTER(revenue_recognition_date_utc) as revenue_recognition_quarter
    FROM revenue_recognition_data 
    WHERE rn = 1
),

-- Get VINs associated with deals
deal_vins AS (
    SELECT DISTINCT
        c.deal_id,
        UPPER(c.vin) as vin,
        c.make,
        c.model,
        c.year,
        c.color
    FROM bronze.leaseend_db_public.cars c
    WHERE c.vin IS NOT NULL 
        AND LENGTH(c.vin) = 17
        AND c.deal_id IS NOT NULL
        AND (c._fivetran_deleted = FALSE OR c._fivetran_deleted IS NULL)
)

-- Final result: VINs grouped by revenue recognition period
SELECT 
    frr.revenue_recognition_period,
    frr.revenue_recognition_year,
    frr.revenue_recognition_month,
    frr.revenue_recognition_quarter,
    frr.revenue_recognition_date_utc,
    dv.vin,
    dv.make,
    dv.model,
    dv.year,
    dv.color,
    dv.deal_id
FROM first_revenue_recognition frr
INNER JOIN deal_vins dv ON frr.deal_id = dv.deal_id
ORDER BY 
    frr.revenue_recognition_year DESC,
    frr.revenue_recognition_month DESC,
    dv.vin; 