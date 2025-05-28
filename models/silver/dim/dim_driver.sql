-- models/silver/dim/dim_driver.sql
-- Dimension table for drivers sourced from bronze tables

-- 1. Define Table Structure
CREATE TABLE IF NOT EXISTS silver.finance.dim_driver (
  driver_key STRING NOT NULL, -- Natural key (source system customer_id)
  first_name STRING,
  middle_name STRING,
  last_name STRING,
  name_suffix STRING,
  email STRING,
  phone_number STRING,
  home_phone_number STRING,
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  dob DATE,
  marital_status STRING,
  age INT,
  finscore DOUBLE,
  _source_table STRING, -- Metadata: Originating table
  _load_timestamp TIMESTAMP -- Metadata: When the record was loaded/updated
)
USING DELTA
PARTITIONED BY (state) -- Partitioning by state for query optimization
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Implement MERGE logic for incremental updates (SCD Type 1)
MERGE INTO silver.finance.dim_driver AS target
USING (
  -- Source query: Select latest distinct driver data from bronze sources
  WITH customer_data AS (
    SELECT
      c.id AS customer_id,
      c.first_name,
      c.middle_name,
      c.last_name,
      c.name_suffix,
      c.email,
      c.phone_number,
      c.home_phone_number,
      c.dob,
      c.marital_status,
      c.updated_at,
      ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.customers c
    WHERE c.id IS NOT NULL
  ),
  address_data AS (
    SELECT
      a.customer_id,
      a.city,
      a.state,
      a.zip,
      a.county,
      a.updated_at,
      ROW_NUMBER() OVER (PARTITION BY a.customer_id ORDER BY a.updated_at DESC) as rn
    FROM bronze.leaseend_db_public.addresses a
    WHERE a.address_type = 'current'
      AND a.customer_id IS NOT NULL
  ),
  finscore_data AS (
    SELECT 
      m.from_id as customer_id,
      oc.finscore,
      ROW_NUMBER() OVER (PARTITION BY m.from_id ORDER BY 
        CASE WHEN oc.finscore IS NOT NULL THEN oc.source_date END DESC) as rn
    FROM silver.mapper.global_mapper m
    INNER JOIN silver.contact_db.contact_snapshot oc ON m.to_id = oc.contact_id
    WHERE m.from_type = 'CUSTOMER' 
      AND m.to_type = 'CONTACT'
  ),
  deal_dates AS (
    SELECT 
      d.customer_id,
      MIN(d.creation_date_utc) as first_deal_date
    FROM bronze.leaseend_db_public.deals d
    WHERE d.customer_id IS NOT NULL
      AND d.creation_date_utc IS NOT NULL
    GROUP BY d.customer_id
  )
  SELECT
    CAST(cd.customer_id AS STRING) AS driver_key,
    cd.first_name,
    cd.middle_name,
    cd.last_name,
    cd.name_suffix,
    cd.email,
    cd.phone_number,
    cd.home_phone_number,
    ad.city,
    ad.state,
    ad.zip,
    ad.county,
    CASE 
      WHEN cd.dob < '1900-01-01' THEN NULL 
      ELSE cd.dob 
    END as dob,
    cd.marital_status,
    CASE 
      WHEN cd.dob IS NOT NULL AND dd.first_deal_date IS NOT NULL 
        AND cd.dob >= '1900-01-01'
      THEN CAST(FLOOR(MONTHS_BETWEEN(dd.first_deal_date, cd.dob) / 12) AS INT)
      ELSE NULL 
    END as age,
    fs.finscore,
    'bronze.leaseend_db_public.customers' AS _source_table
  FROM customer_data cd
  LEFT JOIN address_data ad ON cd.customer_id = ad.customer_id AND ad.rn = 1
  LEFT JOIN finscore_data fs ON cd.customer_id = fs.customer_id AND fs.rn = 1
  LEFT JOIN deal_dates dd ON cd.customer_id = dd.customer_id
  WHERE cd.rn = 1
) AS source
-- Match on the natural key
ON target.driver_key = source.driver_key

-- Update existing records if any attribute changed (simple overwrite)
WHEN MATCHED AND (
    target.first_name <> source.first_name OR
    target.last_name <> source.last_name OR
    target.email <> source.email OR
    target.phone_number <> source.phone_number OR
    target.city <> source.city OR
    target.state <> source.state OR
    target.zip <> source.zip OR
    target.county <> source.county OR
    target.dob <> source.dob OR
    target.marital_status <> source.marital_status OR
    target.age <> source.age OR
    target.finscore <> source.finscore OR
    (target.middle_name IS NULL AND source.middle_name IS NOT NULL) OR 
    (target.middle_name IS NOT NULL AND source.middle_name IS NULL) OR 
    (target.middle_name <> source.middle_name) OR
    (target.name_suffix IS NULL AND source.name_suffix IS NOT NULL) OR 
    (target.name_suffix IS NOT NULL AND source.name_suffix IS NULL) OR 
    (target.name_suffix <> source.name_suffix) OR
    (target.home_phone_number IS NULL AND source.home_phone_number IS NOT NULL) OR 
    (target.home_phone_number IS NOT NULL AND source.home_phone_number IS NULL) OR 
    (target.home_phone_number <> source.home_phone_number)
  ) THEN
  UPDATE SET
    target.first_name = source.first_name,
    target.middle_name = source.middle_name,
    target.last_name = source.last_name,
    target.name_suffix = source.name_suffix,
    target.email = source.email,
    target.phone_number = source.phone_number,
    target.home_phone_number = source.home_phone_number,
    target.city = source.city,
    target.state = source.state,
    target.zip = source.zip,
    target.county = source.county,
    target.dob = source.dob,
    target.marital_status = source.marital_status,
    target.age = source.age,
    target.finscore = source.finscore,
    target._source_table = source._source_table,
    target._load_timestamp = CURRENT_TIMESTAMP()

-- Insert new records (driver_key is the natural key)
WHEN NOT MATCHED THEN
  INSERT (
    driver_key,
    first_name,
    middle_name,
    last_name,
    name_suffix,
    email,
    phone_number,
    home_phone_number,
    city,
    state,
    zip,
    county,
    dob,
    marital_status,
    age,
    finscore,
    _source_table,
    _load_timestamp
  )
  VALUES (
    source.driver_key,
    source.first_name,
    source.middle_name,
    source.last_name,
    source.name_suffix,
    source.email,
    source.phone_number,
    source.home_phone_number,
    source.city,
    source.state,
    source.zip,
    source.county,
    source.dob,
    source.marital_status,
    source.age,
    source.finscore,
    source._source_table,
    CURRENT_TIMESTAMP()
  ); 