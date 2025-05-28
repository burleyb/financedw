-- models/silver/fact/fact_deal_commissions.sql
-- Silver layer fact table for deal commissions reading from bronze sources

CREATE TABLE IF NOT EXISTS silver.finance.fact_deal_commissions (
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
COMMENT 'Silver layer fact table storing commission amounts related to deals'
PARTITIONED BY (commission_date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Merge incremental changes from bronze sources
MERGE INTO silver.finance.fact_deal_commissions AS target
USING (
  WITH deal_data AS (
    SELECT
      d.id,
      d.setter_id,
      d.closer_id,
      d.closer2_id,
      d.creation_date_utc,
      d.completion_date_utc,
      d.state as deal_state,
      d.updated_at,
      ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY d.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.deals d
    WHERE d.id IS NOT NULL
      AND d._fivetran_deleted = FALSE
  ),
  financial_data AS (
    SELECT
      fi.deal_id,
      fi.setter_commission,
      fi.closer_commission,
      fi.commissionable_gross_revenue,
      fi.updated_at,
      ROW_NUMBER() OVER (PARTITION BY fi.deal_id ORDER BY fi.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.financial_infos fi
    WHERE fi.deal_id IS NOT NULL
      AND fi._fivetran_deleted = FALSE
      -- Filter for deals with commission information
      AND (fi.setter_commission IS NOT NULL 
           OR fi.closer_commission IS NOT NULL 
           OR fi.commissionable_gross_revenue IS NOT NULL)
  )
  SELECT DISTINCT
    CAST(dd.id AS STRING) AS deal_key,
    COALESCE(CAST(dd.setter_id AS STRING), 'unknown') AS employee_setter_key,
    COALESCE(CAST(dd.closer_id AS STRING), 'unknown') AS employee_closer_key,
    COALESCE(CAST(dd.closer2_id AS STRING), 'unknown') AS employee_closer2_key,
    COALESCE(
      CAST(DATE_FORMAT(dd.completion_date_utc, 'yyyyMMdd') AS INT),
      CAST(DATE_FORMAT(dd.creation_date_utc, 'yyyyMMdd') AS INT),
      0
    ) AS commission_date_key,
    COALESCE(
      CAST(DATE_FORMAT(dd.completion_date_utc, 'HHmmss') AS INT),
      CAST(DATE_FORMAT(dd.creation_date_utc, 'HHmmss') AS INT),
      0
    ) AS commission_time_key,

    -- Measures (multiply by 100 and cast to BIGINT, use COALESCE to default nulls to zero)
    CAST(COALESCE(fd.setter_commission, 0) * 100 AS BIGINT) AS setter_commission_amount,
    CAST(
      CASE 
        WHEN dd.closer2_id IS NOT NULL THEN COALESCE(fd.closer_commission, 0) / 2
        ELSE COALESCE(fd.closer_commission, 0)
      END * 100 AS BIGINT
    ) AS closer_commission_amount,
    CAST(
      CASE 
        WHEN dd.closer2_id IS NOT NULL THEN COALESCE(fd.closer_commission, 0) / 2
        ELSE 0
      END * 100 AS BIGINT
    ) AS closer2_commission_amount,
    CAST(COALESCE(fd.commissionable_gross_revenue, 0) * 100 AS BIGINT) AS commissionable_gross_revenue_amount,

    'bronze.leaseend_db_public.deals JOIN bronze.leaseend_db_public.financial_infos' as _source_table

  FROM deal_data dd
  LEFT JOIN financial_data fd ON dd.id = fd.deal_id AND fd.rn = 1
  WHERE dd.rn = 1
    AND dd.deal_state IS NOT NULL 
    AND dd.id != 0

) AS source
ON target.deal_key = source.deal_key
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
    target._load_timestamp = CURRENT_TIMESTAMP()

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
    CURRENT_TIMESTAMP()
  ); 