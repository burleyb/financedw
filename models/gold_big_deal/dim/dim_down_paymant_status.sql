CREATE OR REPLACE TABLE gold.finance.dim_down_payment_status
USING DELTA AS
SELECT
  row_number() OVER (ORDER BY down_payment_status) AS down_payment_status_key,
  down_payment_status
FROM (
  SELECT DISTINCT down_payment_status
  FROM silver.deal.big_deal
) t;
