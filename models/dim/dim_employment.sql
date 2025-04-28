CREATE OR REPLACE TABLE gold.finance.dim_employment
USING DELTA AS
SELECT
  monotonically_increasing_id()    AS employment_key,
  employer,
  job_title,
  work_phone_number,
  years_at_job,
  months_at_job,
  gross_income,
  pay_frequency,
  employment_status
FROM silver.deal.big_deal
GROUP BY
  employer, job_title, work_phone_number,
  years_at_job, months_at_job,
  gross_income, pay_frequency, employment_status;