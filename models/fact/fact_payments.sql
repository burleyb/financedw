-- models/fact/fact_payments.sql
-- Fact table for payment transactions

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS gold.fact_payments (
  transaction_id_source STRING, -- Degenerate dimension
  driver_key INT,
  event_date_key INT,
  due_date_key INT,
  deal_id_source INT, -- Degenerate dimension link to deals
  payment_amount DECIMAL(10, 2),
  payment_status STRING, -- Consider making a dimension
  days_late INT,
  _created_at TIMESTAMP,
  _updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Fact table containing payment transaction details.';

-- 2. Implement MERGE logic for incremental updates
MERGE INTO gold.fact_payments AS target
USING (
  -- Source query: Select payment data and join dimensions
  SELECT
    te.transaction_id AS transaction_id_source,
    COALESCE(dr.driver_key, -1) AS driver_key,
    COALESCE(dd_event.date_key, -1) AS event_date_key,
    COALESCE(dd_due.date_key, -1) AS due_date_key,
    te.deal_id AS deal_id_source,
    CAST(te.amount AS DECIMAL(10, 2)) AS payment_amount,
    te.status AS payment_status,
    DATEDIFF(te.event_date, te.due_date) AS days_late,
    te.event_date -- Assuming event_date tracks updates
  FROM silver.deal.transaction_event te
  LEFT JOIN gold.dim_date dd_event ON DATE(te.event_date) = dd_event.date_actual
  LEFT JOIN gold.dim_date dd_due ON DATE(te.due_date) = dd_due.date_actual
  LEFT JOIN gold.dim_driver dr ON te.customer_id = dr.driver_id_source
  WHERE te.transaction_type = 'Payment' -- Filter for payment events
  -- Optional: Add incremental filtering based on event_date or another timestamp
) AS source
ON target.transaction_id_source = source.transaction_id_source

-- Update existing payments
WHEN MATCHED THEN
  UPDATE SET
    target.driver_key = source.driver_key,
    target.event_date_key = source.event_date_key,
    target.due_date_key = source.due_date_key,
    target.deal_id_source = source.deal_id_source,
    target.payment_amount = source.payment_amount,
    target.payment_status = source.payment_status,
    target.days_late = source.days_late,
    target._updated_at = CURRENT_TIMESTAMP()

-- Insert new payments
WHEN NOT MATCHED THEN
  INSERT (
    transaction_id_source, driver_key, event_date_key, due_date_key, 
    deal_id_source, payment_amount, payment_status, days_late, 
    _created_at, _updated_at
  )
  VALUES (
    source.transaction_id_source, source.driver_key, source.event_date_key, source.due_date_key, 
    source.deal_id_source, source.payment_amount, source.payment_status, source.days_late, 
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );

-- Note: This DDL only creates the table structure.
-- Populating it requires an ETL process that handles:
-- 1. Sourcing from bronze.leaseend_db_public.payments.
-- 2. Looking up surrogate keys (dim_date, dim_customer) and deal_id.
-- 3. Handling NULLs.
-- 4. Casting data types and cleansing.
-- 5. Decide if attributes like status/type should be moved to dedicated dimensions.