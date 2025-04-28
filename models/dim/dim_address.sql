CREATE OR REPLACE TABLE gold.finance.dim_address
USING DELTA AS
SELECT
  monotonically_increasing_id() AS address_key,
  address_line,
  address_line_2,
  city,
  state,
  zip,
  county,
  residence_type,
  years_at_home,
  months_at_home,
  time_zone,
  'bronze.leaseend_db_public.addresses' as _source_table,
FROM silver.deal.big_deal
GROUP BY
  address_line, address_line_2, city, state,
  zip, county, residence_type, years_at_home,
  months_at_home, time_zone;