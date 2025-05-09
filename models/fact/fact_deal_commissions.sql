-- models/fact/fact_deal_commissions.sql
-- Fact table for deal commissions

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.finance.fact_deal_commissions (
  -- Keys
  deal_key STRING NOT NULL, -- FK to dim_deal
  employee_setter_key STRING, -- FK to dim_employee (if setter is tracked)
  employee_closer_key STRING, -- FK to dim_employee
  employee_closer2_key STRING, -- FK to dim_employee
  commission_date_key INT, -- FK to dim_date (likely completion date)
  commission_time_key INT, -- FK to dim_time (likely completion time)

  -- Measures (Stored as BIGINT cents)
  setter_commission_amount BIGINT,
  closer_commission_amount BIGINT,
  closer2_commission_amount BIGINT,
  commissionable_gross_revenue_amount BIGINT,

  -- Metadata
  _source_table STRING,
  _load_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Fact table storing commission amounts related to deals.'
PARTITIONED BY (commission_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Merge incremental changes
MERGE INTO gold.finance.fact_deal_commissions AS target
USING (
  SELECT DISTINCT
    sbd.id AS deal_key,
    -- Fetch setter and closer from bronze.deals
    COALESCE(bld.setter_id, 'No Setter') AS employee_setter_key,
    COALESCE(bld.closer_id, 'No Closer') AS employee_closer_key,
    COALESCE(bld.closer2_id, 'No Closer2') AS employee_closer2_key,
    CAST(DATE_FORMAT(COALESCE(sbd.completion_date_utc, sbd.creation_date_utc), 'yyyyMMdd') AS INT) AS commission_date_key,
    CAST(DATE_FORMAT(COALESCE(sbd.completion_date_utc, sbd.creation_date_utc), 'HHmmss') AS INT) AS commission_time_key,

    -- Measures (multiply by 100 and cast to BIGINT, use COALESCE to default nulls to zero)
    CAST(COALESCE(sbd.setter_commission, 0) * 100 AS BIGINT) AS setter_commission_amount,
    CAST(
      CASE 
        WHEN bld.closer2_id IS NOT NULL THEN COALESCE(sbd.closer_commission, 0) / 2
        ELSE COALESCE(sbd.closer_commission, 0)
      END * 100 AS BIGINT
    ) AS closer_commission_amount,
    CAST(
      CASE 
        WHEN bld.closer2_id IS NOT NULL THEN COALESCE(sbd.closer_commission, 0) / 2
        ELSE 0
      END * 100 AS BIGINT
    ) AS closer2_commission_amount,
    CAST(COALESCE(sbd.commissionable_gross_revenue, 0) * 100 AS BIGINT) AS commissionable_gross_revenue_amount,

    'silver.deal.big_deal JOIN bronze.leaseend_db_public.deals' as _source_table,
    CURRENT_TIMESTAMP() as _load_timestamp
  FROM silver.deal.big_deal sbd
  LEFT JOIN bronze.leaseend_db_public.deals bld ON sbd.id = bld.id -- Join silver big_deal with bronze deals
  -- Filter for relevant deals (e.g., those with commissions or recent updates)
  WHERE sbd.deal_state IS NOT NULL AND sbd.id != 0
  -- Ensure only the latest version of each deal is processed if source has duplicates per batch
  QUALIFY ROW_NUMBER() OVER (PARTITION BY sbd.id ORDER BY sbd.state_asof_utc DESC) = 1

) AS source
ON target.deal_key = source.deal_key
   -- Add date key to match for potential updates on the same deal/day
   AND target.commission_date_key = source.commission_date_key

-- Update existing commission records if amounts change
WHEN MATCHED AND (
    target.setter_commission_amount <> source.setter_commission_amount OR
    target.closer_commission_amount <> source.closer_commission_amount OR
    target.closer2_commission_amount <> source.closer2_commission_amount OR
    target.commissionable_gross_revenue_amount <> source.commissionable_gross_revenue_amount OR
    target.employee_setter_key <> source.employee_setter_key OR
    target.employee_closer_key <> source.employee_closer_key OR
    target.employee_closer2_key <> source.employee_closer2_key
  ) THEN
  UPDATE SET
    target.employee_setter_key = source.employee_setter_key,
    target.employee_closer_key = source.employee_closer_key,
    target.employee_closer2_key = source.employee_closer2_key,
    target.commission_time_key = source.commission_time_key,
    target.setter_commission_amount = source.setter_commission_amount,
    target.closer_commission_amount = source.closer_commission_amount,
    target.closer2_commission_amount = source.closer2_commission_amount,
    target.commissionable_gross_revenue_amount = source.commissionable_gross_revenue_amount,
    target._source_table = source._source_table,
    target._load_timestamp = source._load_timestamp

-- Insert new commission records
WHEN NOT MATCHED THEN
  INSERT (
    deal_key,
    employee_setter_key,
    employee_closer_key,
    employee_closer2_key,
    commission_date_key,
    commission_time_key,
    setter_commission_amount,
    closer_commission_amount,
    closer2_commission_amount,
    commissionable_gross_revenue_amount,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.deal_key,
    source.employee_setter_key,
    source.employee_closer_key,
    source.employee_closer2_key,
    source.commission_date_key,
    source.commission_time_key,
    source.setter_commission_amount,
    source.closer_commission_amount,
    source.closer2_commission_amount,
    source.commissionable_gross_revenue_amount,
    source._source_table,
    source._load_timestamp
  ); 