-- silver.deal.big_deal definition

CREATE TABLE silver.deal.big_deal (
  id INT,
  deal_state STRING,
  creation_date_utc TIMESTAMP,
  completion_date_utc TIMESTAMP,
  type STRING,
  source STRING,
  state_asof_utc TIMESTAMP,
  customer_id INT,
  first_name STRING,
  middle_name STRING,
  last_name STRING,
  name_suffix STRING,
  dob DATE,
  marital_status STRING,
  phone_number STRING,
  home_phone_number STRING,
  email STRING,
  age BIGINT,
  finscore STRING,
  address_line STRING,
  address_line_2 STRING,
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  residence_type STRING,
  years_at_home INT,
  months_at_home INT,
  monthly_payment DECIMAL(8,2),
  time_zone STRING,
  employer STRING,
  job_title STRING,
  work_phone_number STRING,
  years_at_job SMALLINT,
  months_at_job SMALLINT,
  gross_income DECIMAL(11,2),
  pay_frequency STRING,
  employment_status STRING,
  color STRING,
  mileage INT,
  book_value DECIMAL(9,2),
  vin STRING,
  make STRING,
  model STRING,
  retail_book_value DECIMAL(9,2),
  fuel_type STRING,
  vehicle_type STRING,
  kbb_valuation_date DATE,
  kbb_vehicle_name STRING,
  kbb_lending_mileage_adjustment DECIMAL(8,2),
  kbb_lending_option_adjustment DECIMAL(8,2),
  kbb_retail_mileage_adjustment DECIMAL(8,2),
  kbb_retail_option_adjustment DECIMAL(8,2),
  kbb_trim_name STRING,
  mmr DECIMAL(8,2),
  license_plate_number STRING,
  license_plate_state STRING,
  registration_expiration TIMESTAMP,
  odometer_status STRING,
  jdp_adjusted_clean_retail INT,
  jdp_adjusted_clean_trade INT,
  jdp_mileage_adjustment INT,
  jdp_trim_body STRING,
  jdp_valuation_date DATE,
  jdp_vehicle_accessories STRING,
  jdp_vehicle_accessories_object STRING,
  model_year INT,
  money_down DECIMAL(8,2),
  title DECIMAL(8,2),
  total_fee_amount DECIMAL(8,2),
  doc_fee DECIMAL(8,2),
  vsc_price DECIMAL(8,2),
  vsc_cost DECIMAL(8,2),
  gap_price DECIMAL(8,2),
  gap_cost DECIMAL(8,2),
  days_to_payment INT,
  first_payment_date DATE,
  sell_rate DECIMAL(4,2),
  term INT,
  bank STRING,
  vsc_term STRING,
  payment DECIMAL(8,2),
  amount_financed DECIMAL(8,2),
  buy_rate DECIMAL(4,2),
  profit DECIMAL(8,2),
  option_type STRING,
  setter_commission DECIMAL(8,2),
  closer_commission DECIMAL(8,2),
  plate_transfer BOOLEAN,
  atc_quote_name STRING,
  sales_tax_rate DECIMAL(10,4),
  reserve DECIMAL(8,2),
  vsc_type STRING,
  title_only BOOLEAN,
  processor STRING,
  commissionable_gross_revenue DECIMAL(8,2),
  ns_invoice_date DATE,
  base_tax_amount DECIMAL(10,2),
  warranty_tax_amount DECIMAL(10,2),
  tax_processor STRING,
  fee_processor STRING,
  com_rate_markup INT,
  approval_on_deal_processing TIMESTAMP,
  finished_documents_screen TIMESTAMP,
  buyer_not_lessee BOOLEAN,
  reached_documents_screen TIMESTAMP,
  down_payment_status STRING,
  bank_fees DECIMAL(8,2),
  user_entered_reserve DECIMAL(8,2),
  card_payment_amount_limit FLOAT,
  registration_transfer_fee DECIMAL(8,2),
  title_fee DECIMAL(8,2),
  new_registration_fee DECIMAL(8,2),
  title_registration_option STRING,
  pen_maintenance_contract_number STRING,
  credit_card_payment_amount_limit FLOAT,
  quick_notes STRING,
  needs_temporary_registration_tags BOOLEAN,
  ally_fees INT,
  rpt DECIMAL(15,2),
  vsc_rev DECIMAL(12,2),
  gap_rev DECIMAL(12,2),
  source_name STRING,
  other_source_description STRING,
  lienholder_name STRING,
  vehicle_payoff DECIMAL(9,2),
  good_through_date DATE,
  lease_term INT,
  remaining_payments INT,
  msrp DECIMAL(8,2),
  residual_percentage DECIMAL(8,2),
  sales_price DECIMAL(8,2),
  residual_amount DECIMAL(8,2),
  estimated_payoff DECIMAL(8,2),
  vehicle_cost DECIMAL(8,2),
  old_lease_payment DECIMAL(8,2),
  next_payment_date DATE,
  user_entered_total_payoff DECIMAL(8,2),
  lienholder_slug STRING,
  row_num INT,
  ns_date TIMESTAMP,
  `4105_rev_reserve` DOUBLE,
  reserve_bonus_rev_4106 DOUBLE,
  reserve_chargeback_rev_4107 DOUBLE,
  reserve_total_rev DOUBLE,
  vsc_rev_4110 DOUBLE,
  vsc_advance_rev_4110a DOUBLE,
  vsc_volume_bonus_rev_4110b DOUBLE,
  vsc_cost_rev_4110c DOUBLE,
  vsc_chargeback_rev_4111 DOUBLE,
  vsc_total_rev DOUBLE,
  gap_rev_4120 DOUBLE,
  gap_advance_rev_4120a DOUBLE,
  gap_volume_bonus_rev_4120b DOUBLE,
  gap_cost_rev_4120c DOUBLE,
  gap_chargeback_rev_4121 DOUBLE,
  gap_total_rev DOUBLE,
  doc_fees_rev_4130 DOUBLE,
  doc_fees_chargeback_rev_4130c DOUBLE,
  titling_fees_rev_4141 DOUBLE,
  `doc_&_title_total_rev` DOUBLE,
  total_revenue DOUBLE,
  funding_clerks_5301 DOUBLE,
  commission_5302 DOUBLE,
  sales_guarantee_5303 DOUBLE,
  ic_payoff_team_5304 DOUBLE,
  outbound_commission_5305 DOUBLE,
  title_clerks_5320 DOUBLE,
  direct_emp_benefits_5330 DOUBLE,
  direct_payroll_tax_5340 DOUBLE,
  direct_people_cost DOUBLE,
  payoff_variance_5400 DOUBLE,
  sales_tax_variance_5401 DOUBLE,
  registration_variance_5402 DOUBLE,
  customer_experience_5403 DOUBLE,
  penalties_5404 DOUBLE,
  payoff_variance_total DOUBLE,
  postage_5510 DOUBLE,
  bank_buyout_fees_5520 DOUBLE,
  other_cor_total DOUBLE,
  vsc_cor_5110 DOUBLE,
  vsc_advance_5110a DOUBLE,
  vsc_total DOUBLE,
  gap_cor_5120 DOUBLE,
  gap_advance_5120a DOUBLE,
  gap_total DOUBLE,
  cor_total DOUBLE,
  gross_profit DOUBLE,
  gross_margin DOUBLE,
  repo DOUBLE)
USING delta
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');

-- bronze.leaseend_db_public.acquisition_leads definition

CREATE TABLE bronze.leaseend_db_public.acquisition_leads (
  id INT,
  temporary_info_id STRING,
  condition_report_id INT,
  name STRING,
  phone_number STRING,
  email STRING,
  address_line STRING,
  city STRING,
  state STRING,
  zip STRING,
  vin STRING,
  vehicle_make STRING,
  vehicle_model STRING,
  vehicle_trim STRING,
  vehicle_year STRING,
  vehicle_mileage STRING,
  vehicle_color STRING,
  lienholder STRING,
  good_through_date TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  address_line_2 STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.acquisition_params definition

CREATE TABLE bronze.leaseend_db_public.acquisition_params (
  id INT,
  percentage_of_appraised_value FLOAT,
  disposition_fee FLOAT,
  car_value_buffer FLOAT,
  mileage_multiplier FLOAT,
  mileage_limit FLOAT,
  low_score_limit FLOAT,
  average_transport_cost FLOAT,
  average_auction_fees FLOAT,
  nissan_fee FLOAT,
  sedan_reduction FLOAT,
  suv_and_truck_reduction FLOAT,
  accident_fee FLOAT,
  cr_score_offset FLOAT,
  max_purchase_price FLOAT,
  disabled_makes STRING,
  disabled_lienholders STRING,
  created_by STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.addresses definition

CREATE TABLE bronze.leaseend_db_public.addresses (
  id INT,
  address_line STRING,
  zip STRING,
  city STRING,
  state STRING,
  county STRING,
  customer_id INT,
  residence_type STRING,
  years_at_home INT,
  months_at_home INT,
  monthly_payment DECIMAL(8,2),
  moved_states BOOLEAN,
  address_type STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  address_line_2 STRING,
  time_zone STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.affiliates definition

CREATE TABLE bronze.leaseend_db_public.affiliates (
  id INT,
  name STRING,
  external_id STRING,
  lead_request STRING,
  decision_response STRING,
  decision_accepted BOOLEAN,
  decision_error STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  le_error STRING,
  affiliate_provider_id INT,
  attempted_import_at TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.affiliate_api_keys definition

CREATE TABLE bronze.leaseend_db_public.affiliate_api_keys (
  id INT,
  api_key_hash STRING,
  name STRING,
  affiliate_provider_id INT,
  is_active BOOLEAN,
  last_used_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.affiliate_providers definition

CREATE TABLE bronze.leaseend_db_public.affiliate_providers (
  id INT,
  name STRING,
  slug STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.appointments definition

CREATE TABLE bronze.leaseend_db_public.appointments (
  id INT,
  deal_id INT,
  type STRING,
  date_utc TIMESTAMP,
  tz STRING,
  previous_state STRING,
  creator_id STRING,
  notes STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.articles definition

CREATE TABLE bronze.leaseend_db_public.articles (
  id INT,
  title STRING,
  headline STRING,
  url STRING,
  created_at TIMESTAMP,
  com_visible BOOLEAN,
  updated_at TIMESTAMP,
  thumbnail_key STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.auctions definition

CREATE TABLE bronze.leaseend_db_public.auctions (
  id INT,
  name STRING,
  address_line STRING,
  zip STRING,
  city STRING,
  state STRING,
  phone_number STRING,
  average_transport_cost INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.auto_assign_eligibilities definition

CREATE TABLE bronze.leaseend_db_public.auto_assign_eligibilities (
  id INT,
  deal_id INT,
  fs_id STRING,
  assigned_deal BOOLEAN,
  weight FLOAT,
  is_eligible BOOLEAN,
  ineligibility_reason STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.auto_structure_credit_score_tiers definition

CREATE TABLE bronze.leaseend_db_public.auto_structure_credit_score_tiers (
  id INT,
  min INT,
  max INT,
  should_autostructure BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.auto_structure_credit_score_tiers_banks definition

CREATE TABLE bronze.leaseend_db_public.auto_structure_credit_score_tiers_banks (
  id INT,
  auto_structure_credit_score_tiers_id INT,
  bank_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  auto_structure_type STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.awsdms_ddl_audit definition

CREATE TABLE bronze.leaseend_db_public.awsdms_ddl_audit (
  c_key BIGINT,
  c_time TIMESTAMP,
  c_user STRING,
  c_txn STRING,
  c_tag STRING,
  c_oid INT,
  c_name STRING,
  c_schema STRING,
  c_ddlqry STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.banks definition

CREATE TABLE bronze.leaseend_db_public.banks (
  id INT,
  name STRING,
  address STRING,
  phone STRING,
  city STRING,
  state STRING,
  zip STRING,
  r1_fsid STRING,
  auto_structure BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  logo_url STRING,
  active BOOLEAN,
  description STRING,
  sort_order INT,
  display_name STRING,
  use_in_states STRING,
  signing_solution STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  min_credit_score INT,
  auto_structure_refi BOOLEAN,
  auto_structure_buyout BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.bank_markups definition

CREATE TABLE bronze.leaseend_db_public.bank_markups (
  id INT,
  bank_id INT,
  term_min INT,
  term_max INT,
  max_markup DECIMAL(4,3),
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.calls definition

CREATE TABLE bronze.leaseend_db_public.calls (
  id STRING,
  deal_id INT COMMENT 'Deal ID associated with the phone number at the time of the call. Will be populated only if the individual has a deal ID assigned already when they call in. ',
  callstatus STRING COMMENT 'Deprecated',
  direction STRING COMMENT 'Deprecated',
  twilionumber STRING,
  fromnumber STRING,
  callednumber STRING,
  user_id STRING COMMENT 'Deprecated',
  recordingsid STRING,
  recordingurl STRING,
  callduration STRING,
  calledcity STRING,
  calledstate STRING,
  created_at TIMESTAMP,
  selection STRING COMMENT 'Deprecated',
  selection_message STRING,
  pod_id INT COMMENT 'Deprecated',
  callername STRING,
  updated_at TIMESTAMP,
  parent_call_sid STRING COMMENT 'Deprecated',
  sip_call_id STRING,
  flow STRING,
  isnewcustomer BOOLEAN,
  outcome STRING,
  conferenceid STRING,
  new_customer_call_outcome STRING,
  twilio_friendly_name STRING,
  new_customer_call_outcome_description STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  caller_state STRING,
  caller_city STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.calls_presented definition

CREATE TABLE bronze.leaseend_db_public.calls_presented (
  id INT,
  call_id STRING,
  user_id STRING,
  answered BOOLEAN,
  answer_time FLOAT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.cars definition

CREATE TABLE bronze.leaseend_db_public.cars (
  id INT,
  color STRING,
  mileage INT,
  book_value DECIMAL(9,2),
  vin STRING,
  year STRING,
  make STRING,
  model STRING,
  deal_id INT,
  retail_book_value DECIMAL(9,2),
  fuel_type STRING,
  vehicle_type STRING,
  kbb_valuation_date DATE,
  kbb_vehicle_name STRING,
  kbb_lending_mileage_adjustment DECIMAL(8,2),
  kbb_lending_option_adjustment DECIMAL(8,2),
  kbb_retail_mileage_adjustment DECIMAL(8,2),
  kbb_retail_option_adjustment DECIMAL(8,2),
  kbb_selected_options STRING,
  kbb_trim_name STRING,
  mmr DECIMAL(8,2),
  hubspot_id STRING,
  license_plate_number STRING,
  license_plate_state STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  registration_expiration TIMESTAMP,
  odometer_status STRING,
  jdp_adjusted_clean_retail INT,
  jdp_adjusted_clean_trade INT,
  jdp_mileage_adjustment INT,
  jdp_trim_body STRING,
  jdp_valuation_date DATE,
  jdp_vehicle_accessories STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  kbb_selected_options_object STRING,
  jdp_vehicle_accessories_object STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.cdr_conferences definition

CREATE TABLE bronze.leaseend_db_public.cdr_conferences (
  id STRING,
  from STRING,
  name STRING,
  deal_id INT,
  ended_at TIMESTAMP,
  recording_url STRING,
  recording_start_time TIMESTAMP,
  recording_duration STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.cdr_participants definition

CREATE TABLE bronze.leaseend_db_public.cdr_participants (
  id INT,
  conference_id STRING,
  participant_user_id STRING,
  participant_number STRING,
  concluded_at TIMESTAMP,
  transferred BOOLEAN,
  transferred_from_number STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  call_id STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  participant_customer_id INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.condition_reports definition

CREATE TABLE bronze.leaseend_db_public.condition_reports (
  id INT,
  deal_id INT,
  accidents STRING,
  tires STRING,
  exterior STRING,
  interior STRING,
  smoked_in BOOLEAN,
  lights_on_dash BOOLEAN,
  overall_condition STRING,
  score DECIMAL(2,1),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.conferences_in_progress definition

CREATE TABLE bronze.leaseend_db_public.conferences_in_progress (
  id INT,
  from STRING,
  hunt_group_slug STRING,
  deal_id INT,
  agent_user_id STRING,
  agent_number STRING,
  conference_id STRING,
  started_at STRING,
  deal_state STRING,
  customer_name STRING,
  to STRING,
  transferred_from STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_outbound BOOLEAN,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  participants_count INT,
  next_participant STRING,
  deal_type STRING,
  is_thunder BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.consents definition

CREATE TABLE bronze.leaseend_db_public.consents (
  id INT,
  type STRING,
  customer_id INT,
  user_id STRING,
  deal_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  temp_info_id STRING,
  consent_agreement_id INT,
  affiliate_id INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.consent_agreements definition

CREATE TABLE bronze.leaseend_db_public.consent_agreements (
  id INT,
  text STRING,
  type STRING,
  version STRING,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.credit_applications definition

CREATE TABLE bronze.leaseend_db_public.credit_applications (
  id INT,
  deal_id INT,
  r1_conversation_id STRING,
  r1_credit_app_guid STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  credit_application_data STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  credit_application_submission_id INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.credit_application_submissions definition

CREATE TABLE bronze.leaseend_db_public.credit_application_submissions (
  id INT,
  deal_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.credit_decisions definition

CREATE TABLE bronze.leaseend_db_public.credit_decisions (
  id INT,
  r1_conversation_id STRING,
  application_status STRING,
  term INT,
  annual_percentage_rate DECIMAL(8,2),
  created_at TIMESTAMP,
  r1_fsid STRING,
  updated_at TIMESTAMP,
  r1_application_number STRING,
  r1_credit_decision_data STRING,
  max_markup DECIMAL(4,3),
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  tier STRING,
  balance_amount DECIMAL(10,2),
  lender_fees DECIMAL(10,2))
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.customers definition

CREATE TABLE bronze.leaseend_db_public.customers (
  id INT,
  first_name STRING,
  last_name STRING,
  phone_number STRING,
  email STRING,
  ssn STRING,
  dob DATE,
  home_phone_number STRING,
  relation_to_buyer STRING,
  middle_name STRING,
  hubspot_id STRING,
  no_email BOOLEAN,
  sms_auth0_id STRING,
  has_same_address_as_cobuyer BOOLEAN,
  dl_expiration_date DATE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  marital_status STRING,
  last_authenticated_route STRING,
  dashboard_visited BOOLEAN,
  sent_account_email BOOLEAN,
  auth0_id STRING,
  contact_id STRING,
  credit_700_output STRING,
  name_suffix STRING,
  finished_signing BOOLEAN,
  signature_data_id INT,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  search_vector STRING,
  verified BOOLEAN,
  sent_review_email TIMESTAMP,
  payoff_email_sent_at TIMESTAMP,
  sent_finalized_email TIMESTAMP,
  title_received_email_sent_at TIMESTAMP,
  sent_to_processor_email_sent_at TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.customers_addresses definition

CREATE TABLE bronze.leaseend_db_public.customers_addresses (
  id INT,
  customer_id INT,
  address_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.customers_employments definition

CREATE TABLE bronze.leaseend_db_public.customers_employments (
  id INT,
  customer_id INT,
  employment_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.customers_previous_addresses definition

CREATE TABLE bronze.leaseend_db_public.customers_previous_addresses (
  id INT,
  customer_id INT,
  address_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.customers_previous_employments definition

CREATE TABLE bronze.leaseend_db_public.customers_previous_employments (
  id INT,
  customer_id INT,
  employment_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.custom_addresses definition

CREATE TABLE bronze.leaseend_db_public.custom_addresses (
  id INT,
  type STRING,
  display_name STRING,
  name STRING,
  address_1 STRING,
  city STRING,
  state STRING,
  zip STRING,
  phone STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  address_2 STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.days_closed definition

CREATE TABLE bronze.leaseend_db_public.days_closed (
  id INT,
  date DATE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.dealers definition

CREATE TABLE bronze.leaseend_db_public.dealers (
  id INT,
  name STRING,
  emails STRING,
  states STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.deals definition

CREATE TABLE bronze.leaseend_db_public.deals (
  missing_required_external_documents BOOLEAN,
  completion_date_utc TIMESTAMP,
  r1_contract_validation_errors STRING,
  r1_contract_validation_started_at TIMESTAMP,
  request_boot BOOLEAN,
  document_progress_status STRING,
  lane_arrival_utc TIMESTAMP,
  r1_contract_manually_validated BOOLEAN,
  contact2_id INT,
  _fivetran_deleted BOOLEAN,
  title_clerk2_id STRING,
  creation_date_utc TIMESTAMP,
  source STRING,
  type STRING,
  titling_pod_id INT,
  imported_date_utc TIMESTAMP,
  auto_import_variation STRING,
  id INT,
  state STRING,
  opened_by_financial_specialist STRING,
  best_rate_credit_decision_status STRING,
  r1_contract_generation_date_utc TIMESTAMP,
  cobuyer_id INT,
  r1_contract_validation_warnings_object STRING,
  creation_date_tz STRING,
  r1_contract_validation_warnings STRING,
  set_date TIMESTAMP,
  r1_contract_validation_errors_object STRING,
  r1_jacket_id_created_date_utc TIMESTAMP,
  funding_clerk_id STRING,
  pod_id INT,
  r1_jacket_id STRING,
  setter_id STRING,
  completion_date_tz STRING,
  paperwork_type STRING,
  created_at TIMESTAMP,
  signing_on_com BOOLEAN,
  marketing_source STRING,
  closer_id STRING,
  contact_id INT,
  closer2_id STRING,
  hubspot_id STRING,
  title_clerk_id STRING,
  _fivetran_synced TIMESTAMP,
  updated_at TIMESTAMP,
  r1_contract_worksheet_created_date_utc TIMESTAMP,
  sales_visibility BOOLEAN,
  has_problem BOOLEAN,
  docs_sent_date DATE,
  needs_electronic_signature_verification BOOLEAN,
  customer_id INT,
  lease_id STRING,
  boot_reason STRING,
  import_type STRING,
  shadow_closer_id_buckets STRING,
  shadow_closer_id_personalized STRING,
  structuring_manager_id STRING,
  last_outgoing_communication_date_utc TIMESTAMP,
  default_sort_date TIMESTAMP,
  temporary_registration_tags_user_id STRING,
  called_by_financial_specialist TIMESTAMP,
  duplicated_from_deal_id INT,
  original_lessee_id INT,
  assigned_sms_phone_number STRING,
  affiliate_provider_id STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.deals_media definition

CREATE TABLE bronze.leaseend_db_public.deals_media (
  id INT,
  key STRING,
  type STRING,
  metadata STRING,
  deal_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  verified BOOLEAN,
  state STRING,
  has_digital_signature BOOLEAN,
  has_digital_wet_signature BOOLEAN,
  has_wet_signature BOOLEAN,
  is_notarized BOOLEAN,
  source STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  has_verified_digital_signature BOOLEAN,
  uploaded_by_customer BOOLEAN,
  r1_document_id STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.deals_referrals_sources definition

CREATE TABLE bronze.leaseend_db_public.deals_referrals_sources (
  id INT,
  deal_id INT,
  source_name STRING,
  other_source_description STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.deals_tags definition

CREATE TABLE bronze.leaseend_db_public.deals_tags (
  deal_id INT,
  tag_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.deal_assignment_logs definition

CREATE TABLE bronze.leaseend_db_public.deal_assignment_logs (
  id INT,
  deal_id INT,
  assignment_logs STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  assignment_logs_object STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.deal_contacts definition

CREATE TABLE bronze.leaseend_db_public.deal_contacts (
  id INT,
  deal_id INT,
  relation_to_buyer STRING,
  first_name STRING,
  middle_name STRING,
  last_name STRING,
  phone_number STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  email STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.deal_dates definition

CREATE TABLE bronze.leaseend_db_public.deal_dates (
  id INT,
  deal_id INT,
  dates STRING,
  custom_dates STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.deal_states definition

CREATE TABLE bronze.leaseend_db_public.deal_states (
  id INT,
  state STRING,
  updated_date_utc TIMESTAMP,
  deal_id INT,
  user_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.deal_types definition

CREATE TABLE bronze.leaseend_db_public.deal_types (
  id INT,
  deal_id INT,
  type STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.document_coordinates definition

CREATE TABLE bronze.leaseend_db_public.document_coordinates (
  id INT,
  title STRING,
  name STRING,
  pagenumber INT,
  xposition DECIMAL(6,2),
  yposition DECIMAL(6,2),
  width DECIMAL(6,2),
  height DECIMAL(6,2),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.document_infos definition

CREATE TABLE bronze.leaseend_db_public.document_infos (
  id INT,
  deal_id INT,
  adobe_agreement_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.elt_codes definition

CREATE TABLE bronze.leaseend_db_public.elt_codes (
  id INT,
  state STRING,
  elt STRING,
  bank_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.employments definition

CREATE TABLE bronze.leaseend_db_public.employments (
  employment_type STRING,
  gross_income DECIMAL(11,2),
  _fivetran_deleted BOOLEAN,
  created_at TIMESTAMP,
  pay_frequency STRING,
  months_at_job SMALLINT,
  years_at_job SMALLINT,
  _fivetran_synced TIMESTAMP,
  updated_at TIMESTAMP,
  name STRING,
  phone_number STRING,
  id INT,
  customer_id INT,
  job_title STRING,
  status STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.extracted_document_data definition

CREATE TABLE bronze.leaseend_db_public.extracted_document_data (
  id INT,
  deal_id INT,
  dl_first_name STRING,
  dl_middle_name STRING,
  dl_last_name STRING,
  dl_address STRING,
  dl_city STRING,
  dl_state STRING,
  dl_zip STRING,
  dl_expiration_date STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  insurance_company STRING,
  insurance_company_confidence INT,
  insurance_expiration STRING,
  insurance_expiration_confidence INT,
  insurance_policy_number STRING,
  insurance_policy_number_confidence INT,
  insurance_state STRING,
  insurance_state_confidence INT,
  insurance_name STRING,
  insurance_name_confidence INT,
  insurance_vin STRING,
  insurance_vin_confidence INT,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.feedbacks definition

CREATE TABLE bronze.leaseend_db_public.feedbacks (
  id INT,
  name STRING,
  email STRING,
  rating INT,
  improvements STRING,
  recommend BOOLEAN,
  recommend_reason STRING,
  additional_details STRING,
  ip STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  deal_id INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.financial_infos definition

CREATE TABLE bronze.leaseend_db_public.financial_infos (
  id INT,
  deal_id INT,
  money_down DECIMAL(8,2),
  deprecated_taxes DECIMAL(8,2),
  title DECIMAL(8,2),
  total_fee_amount DECIMAL(8,2),
  doc_fee DECIMAL(8,2),
  vsc_price DECIMAL(8,2),
  vsc_cost DECIMAL(8,2),
  gap_price DECIMAL(8,2),
  gap_cost DECIMAL(8,2),
  days_to_payment INT,
  first_payment_date DATE,
  sell_rate DECIMAL(4,2),
  term INT,
  bank STRING,
  vsc_term STRING,
  payment DECIMAL(8,2),
  amount_financed DECIMAL(8,2),
  buy_rate DECIMAL(4,2),
  profit DECIMAL(8,2),
  option_type STRING,
  setter_commission DECIMAL(8,2),
  closer_commission DECIMAL(8,2),
  deprecated_taxes_no_warranty DECIMAL(10,2),
  plate_transfer BOOLEAN,
  atc_quote_name STRING,
  pen_vsc_session_id STRING,
  pen_vsc_rate_id INT,
  pen_vsc_form_id INT,
  pen_gap_session_id STRING,
  pen_gap_rate_id INT,
  pen_gap_form_id INT,
  pen_vsc_contract_number STRING,
  pen_gap_contract_number STRING,
  sales_tax_rate DECIMAL(10,4),
  reserve DECIMAL(8,2),
  vsc_type STRING,
  title_only BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  processor STRING,
  commissionable_gross_revenue DECIMAL(8,2),
  ns_invoice_date DATE,
  base_tax_amount DECIMAL(10,2),
  warranty_tax_amount DECIMAL(10,2),
  tt_transaction_id STRING,
  tax_processor STRING,
  fee_processor STRING,
  com_rate_markup INT,
  approval_on_deal_processing TIMESTAMP,
  finished_documents_screen TIMESTAMP,
  buyer_not_lessee BOOLEAN,
  selected_credit_decision_id INT,
  reached_documents_screen TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  down_payment_status STRING,
  bank_fees DECIMAL(8,2),
  user_entered_reserve DECIMAL(8,2),
  card_payment_amount_limit FLOAT,
  registration_transfer_fee DECIMAL(8,2),
  title_fee DECIMAL(8,2),
  new_registration_fee DECIMAL(8,2),
  title_registration_option STRING,
  pen_maintenance_contract_number STRING,
  credit_card_payment_amount_limit FLOAT,
  quick_notes STRING,
  needs_temporary_registration_tags BOOLEAN,
  commissionable_profit DECIMAL(8,2))
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.financial_info_acquisitions definition

CREATE TABLE bronze.leaseend_db_public.financial_info_acquisitions (
  id INT,
  deal_id INT,
  kbb_lending DECIMAL(8,2),
  appraised_value DECIMAL(8,2),
  max_cash_to_customer DECIMAL(8,2),
  max_total_cost DECIMAL(8,2),
  cash_to_customer DECIMAL(8,2),
  is_approved BOOLEAN,
  auction_fees DECIMAL(8,2),
  transport_cost DECIMAL(8,2),
  total_cost DECIMAL(8,2),
  auction_id INT,
  gross_profit DECIMAL(8,2),
  sell_price DECIMAL(8,2),
  offer DECIMAL(8,2),
  estimated_dealer_fees DECIMAL(8,2),
  overriding_user_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.flow_notifications definition

CREATE TABLE bronze.leaseend_db_public.flow_notifications (
  id INT,
  deal_id INT,
  name STRING,
  platform STRING,
  sent_at DATE,
  date_created TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.follow_ups definition

CREATE TABLE bronze.leaseend_db_public.follow_ups (
  id INT,
  deal_id INT,
  type STRING,
  status STRING,
  date_utc TIMESTAMP,
  tz STRING,
  creator_id STRING,
  notes STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  note STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.fs_assignment_weights definition

CREATE TABLE bronze.leaseend_db_public.fs_assignment_weights (
  id INT,
  fs_id STRING,
  bucket_weight FLOAT,
  personalized_weight FLOAT,
  bucket_assigned_deals INT,
  personalized_assigned_deals INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  rpa DECIMAL(20,16))
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.holiday_hours definition

CREATE TABLE bronze.leaseend_db_public.holiday_hours (
  id INT,
  date DATE,
  is_closed BOOLEAN,
  open_time STRING,
  closing_time STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.hubspot_lists definition

CREATE TABLE bronze.leaseend_db_public.hubspot_lists (
  id INT,
  hubspot_id STRING,
  phone_number STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  description STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.hunt_groups definition

CREATE TABLE bronze.leaseend_db_public.hunt_groups (
  id INT,
  name STRING,
  slug STRING,
  phone_number STRING,
  default_queue BOOLEAN,
  ring_all BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.hunt_group_numbers definition

CREATE TABLE bronze.leaseend_db_public.hunt_group_numbers (
  id INT,
  hunt_group_id INT,
  phone_number STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.hunt_group_users definition

CREATE TABLE bronze.leaseend_db_public.hunt_group_users (
  id INT,
  hunt_group_id INT,
  user_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.knex_migrations definition

CREATE TABLE bronze.leaseend_db_public.knex_migrations (
  id INT,
  name STRING,
  batch INT,
  migration_time TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.knex_migrations_lock definition

CREATE TABLE bronze.leaseend_db_public.knex_migrations_lock (
  index INT,
  is_locked INT,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.lease_peace definition

CREATE TABLE bronze.leaseend_db_public.lease_peace (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone_number STRING,
  document_url STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.lienholders definition

CREATE TABLE bronze.leaseend_db_public.lienholders (
  id INT,
  name STRING,
  address_line STRING,
  zip STRING,
  city STRING,
  state STRING,
  phone_number STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.machine_to_machine_tokens definition

CREATE TABLE bronze.leaseend_db_public.machine_to_machine_tokens (
  id INT,
  token STRING,
  type STRING,
  expiration_date TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.marketing_opt_events definition

CREATE TABLE bronze.leaseend_db_public.marketing_opt_events (
  id INT,
  email STRING,
  phone_number STRING,
  address_line STRING,
  city STRING,
  state STRING,
  zip STRING,
  action STRING,
  reason STRING,
  source STRING,
  note STRING,
  name STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  email_opt_in_at DATE,
  last_name STRING,
  address_line_2 STRING,
  sms_opt_in_at DATE,
  first_name STRING,
  mail_opt_in_at DATE)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.metrics_pages definition

CREATE TABLE bronze.leaseend_db_public.metrics_pages (
  id INT,
  name STRING,
  link_name STRING,
  iframe_url STRING,
  permission_name STRING,
  sort_order INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.new_appointments definition

CREATE TABLE bronze.leaseend_db_public.new_appointments (
  id INT,
  deal_id INT,
  type STRING,
  status STRING,
  date_utc TIMESTAMP,
  tz STRING,
  note STRING,
  outcome STRING,
  creator_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.notes definition

CREATE TABLE bronze.leaseend_db_public.notes (
  id INT,
  text STRING,
  author_id STRING,
  deal_id INT,
  created_at TIMESTAMP,
  creation_date_tz STRING,
  note_type STRING,
  updated_at TIMESTAMP,
  pinned BOOLEAN,
  notification_id INT,
  phone_number STRING,
  recordingid STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.notifications definition

CREATE TABLE bronze.leaseend_db_public.notifications (
  id INT,
  message STRING,
  seen BOOLEAN,
  deal_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  customer_id INT,
  deal_contact_id INT,
  text_message_id INT,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  call_id STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.notification_subscribers definition

CREATE TABLE bronze.leaseend_db_public.notification_subscribers (
  id INT,
  deal_id INT,
  subscriber_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.offers definition

CREATE TABLE bronze.leaseend_db_public.offers (
  id INT,
  affiliate_id INT,
  istest BOOLEAN,
  spudacus_version STRING,
  index STRING,
  spread DECIMAL(5,2),
  index_value DECIMAL(5,2),
  suggested_markup DECIMAL(5,2),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.operating_hours definition

CREATE TABLE bronze.leaseend_db_public.operating_hours (
  id INT,
  day_of_week STRING,
  open_time STRING,
  close_time STRING,
  tz STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.opt_events definition

CREATE TABLE bronze.leaseend_db_public.opt_events (
  id INT,
  email STRING,
  phone_number STRING,
  address_line STRING,
  city STRING,
  state STRING,
  zip STRING,
  action STRING,
  reason STRING,
  source STRING,
  note STRING,
  first_name STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  last_name STRING,
  address_line_2 STRING,
  email_opt_in_at DATE,
  sms_opt_in_at DATE,
  mail_opt_in_at DATE,
  ip STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  address STRING,
  opt_in_at TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.original_lessees definition

CREATE TABLE bronze.leaseend_db_public.original_lessees (
  id INT,
  first_name STRING,
  last_name STRING,
  phone_number STRING,
  email STRING,
  address_line STRING,
  ssn STRING,
  city STRING,
  state STRING,
  zip STRING,
  county STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.payments definition

CREATE TABLE bronze.leaseend_db_public.payments (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  amount STRING,
  state STRING,
  deal_id INT,
  transfer_id STRING,
  transfer_type STRING,
  transfer_trace_id STRING,
  payment_instrument_id STRING,
  payment_instrument_type STRING,
  masked_account_number STRING,
  last_four STRING,
  account_type STRING,
  bank_code STRING,
  identity_id STRING,
  fraud_session_id STRING,
  idempotency_id STRING,
  plaid_account_id STRING,
  plaid_token STRING,
  failure_code STRING,
  failure_message STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  bank_name STRING,
  receipt_sent_at TIMESTAMP,
  card_type STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.payoffs definition

CREATE TABLE bronze.leaseend_db_public.payoffs (
  id INT,
  lienholder_name STRING,
  account_number STRING,
  vehicle_payoff DECIMAL(9,2),
  car_id INT,
  good_through_date DATE,
  lease_term INT,
  remaining_payments INT,
  msrp DECIMAL(8,2),
  residual_percentage DECIMAL(8,2),
  sales_price DECIMAL(8,2),
  cap_reduction DECIMAL(8,2),
  money_down DECIMAL(8,2),
  money_factor DECIMAL(8,2),
  termination_fees DECIMAL(8,2),
  cap_cost DECIMAL(8,2),
  residual_amount DECIMAL(8,2),
  estimated_payoff DECIMAL(8,2),
  rebates DECIMAL(8,2),
  vehicle_cost DECIMAL(8,2),
  start_date DATE,
  old_lease_payment DECIMAL(8,2),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  phone_number STRING,
  sales_tax_from_payoff DECIMAL(8,2),
  payoff_includes_sales_tax BOOLEAN,
  sales_tax_from_payoff_entered_manually BOOLEAN,
  verification_status STRING,
  next_payment_date DATE,
  user_entered_total_payoff DECIMAL(8,2),
  lienholder_slug STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  lender_name STRING,
  maturity_date DATE)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.payoff_changes definition

CREATE TABLE bronze.leaseend_db_public.payoff_changes (
  id INT,
  payoff_id INT,
  user_id STRING,
  vehicle_payoff DECIMAL(8,2),
  payoff_includes_sales_tax BOOLEAN,
  sales_tax_from_payoff_entered_manually BOOLEAN,
  sales_tax_from_payoff DECIMAL(8,2),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.payoff_requests definition

CREATE TABLE bronze.leaseend_db_public.payoff_requests (
  id INT,
  status STRING,
  vehicle_payoff DECIMAL(10,2),
  sales_tax DECIMAL(8,2),
  completed_date TIMESTAMP,
  good_through_date TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  deal_id INT,
  temporary_info_id STRING,
  fail_reason STRING,
  payoff_clerk_id STRING,
  needs_payoff_documents BOOLEAN,
  payoff_documents_uploaded BOOLEAN,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  requestor_id STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.pods definition

CREATE TABLE bronze.leaseend_db_public.pods (
  hours STRING,
  color STRING,
  manager_commission_rate DECIMAL(8,4),
  _fivetran_deleted BOOLEAN,
  created_at TIMESTAMP,
  vsc_markup INT,
  setter_commission_rate DECIMAL(8,4),
  closer_commission_type STRING,
  archived BOOLEAN,
  _fivetran_synced TIMESTAMP,
  updated_at TIMESTAMP,
  special_commission_rate DECIMAL(8,4),
  special_commission_type STRING,
  phone STRING,
  vsc_multiplier DECIMAL(8,2),
  parent_pod_id INT,
  setter_commission_type STRING,
  name STRING,
  closer_commission_rate DECIMAL(8,4),
  team_type STRING,
  id INT,
  manager_commission_type STRING,
  us_states_object STRING,
  temp_old_titling_pod_id INT,
  problem_solver BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.pods_users definition

CREATE TABLE bronze.leaseend_db_public.pods_users (
  _fivetran_id STRING,
  pod_id INT,
  user_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  team_role STRING,
  id INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.pod_archive_logs definition

CREATE TABLE bronze.leaseend_db_public.pod_archive_logs (
  id INT,
  pod_id INT,
  user_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.potential_users definition

CREATE TABLE bronze.leaseend_db_public.potential_users (
  id INT,
  signup_date TIMESTAMP,
  name STRING,
  email STRING,
  phone STRING,
  zip INT,
  employment_status STRING,
  referral_code STRING,
  reached_out BOOLEAN,
  interview_scheduled BOOLEAN,
  hired BOOLEAN,
  email_created BOOLEAN,
  login_created BOOLEAN,
  slack_created BOOLEAN,
  added_to_testflight BOOLEAN,
  potential_role STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.prequalifications definition

CREATE TABLE bronze.leaseend_db_public.prequalifications (
  id INT,
  customer_id INT,
  credit_score INT,
  output STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  payment_to_income DECIMAL(8,2),
  consent_id INT,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  input STRING,
  ltv DECIMAL(8,2),
  is_hard_pull BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.prequalification_results definition

CREATE TABLE bronze.leaseend_db_public.prequalification_results (
  id INT,
  customer_id INT,
  credit_score INT,
  result STRING,
  failure_reasons STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.products definition

CREATE TABLE bronze.leaseend_db_public.products (
  id INT,
  vsc_input STRING,
  vsc_output STRING,
  gap_input STRING,
  gap_output STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  package_options STRING,
  selected_package STRING,
  gap_selected BOOLEAN,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  gap_output_object STRING,
  vsc_output_object STRING,
  maintenance_output_object STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.proof_of_insurance definition

CREATE TABLE bronze.leaseend_db_public.proof_of_insurance (
  id INT,
  customer_id INT,
  company_name STRING,
  expires DATE,
  policy_number STRING,
  state STRING,
  first_name STRING,
  middle_name STRING,
  last_name STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.r1_contract_validation_errors_logs definition

CREATE TABLE bronze.leaseend_db_public.r1_contract_validation_errors_logs (
  id INT,
  description STRING,
  count INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.reserve_structures definition

CREATE TABLE bronze.leaseend_db_public.reserve_structures (
  id INT,
  bank_id INT,
  flat_percentage DECIMAL(5,4),
  rate_adj_min DECIMAL(5,4),
  rate_adj_max DECIMAL(5,4),
  term_min INT,
  term_max INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.reviews definition

CREATE TABLE bronze.leaseend_db_public.reviews (
  id INT,
  customer_name STRING,
  review STRING,
  com_visible BOOLEAN,
  created_at TIMESTAMP,
  location STRING,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.shipping_labels definition

CREATE TABLE bronze.leaseend_db_public.shipping_labels (
  id INT,
  deal_id INT,
  easypost_id STRING,
  carrier STRING,
  tracker_id STRING,
  tracking_number STRING,
  tracker_url STRING,
  last_status STRING,
  price DECIMAL(10,2),
  service STRING,
  easypost_from_address_id STRING,
  from_address STRING,
  easypost_to_address_id STRING,
  to_name STRING,
  to_address STRING,
  to_city STRING,
  to_state STRING,
  to_zip STRING,
  to_phone STRING,
  file_url STRING,
  is_return_label BOOLEAN,
  easypost_pickup_id STRING,
  easypost_pickup_cost DECIMAL(10,2),
  is_cancelled BOOLEAN,
  created_by_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  purpose STRING,
  address_2 STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.signature_data definition

CREATE TABLE bronze.leaseend_db_public.signature_data (
  id INT,
  signature_data_url STRING,
  signature_strokes STRING,
  initials_data_url STRING,
  initials_strokes STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.signing_audit_logs definition

CREATE TABLE bronze.leaseend_db_public.signing_audit_logs (
  id INT,
  deal_id INT,
  attempt INT,
  r1_jacket_id STRING,
  signing_on_com BOOLEAN,
  contact_method STRING,
  buyer_email STRING,
  cobuyer_email STRING,
  sent_timestamp STRING,
  sent_by STRING,
  events STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.signing_errors definition

CREATE TABLE bronze.leaseend_db_public.signing_errors (
  id INT,
  deal_id INT,
  error_type STRING,
  error_message STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  error_details STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.slack_threads definition

CREATE TABLE bronze.leaseend_db_public.slack_threads (
  id INT,
  deal_id INT,
  thread_ts STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.ssn_logs definition

CREATE TABLE bronze.leaseend_db_public.ssn_logs (
  id INT,
  user_id STRING,
  customer_id INT,
  temp_info_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  original_lessee_id INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.structuring_follow_ups definition

CREATE TABLE bronze.leaseend_db_public.structuring_follow_ups (
  id INT,
  deal_id INT,
  status STRING,
  creator_id STRING,
  notes STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.tags definition

CREATE TABLE bronze.leaseend_db_public.tags (
  id INT,
  slug STRING,
  display_name STRING,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  is_dashboard_visible BOOLEAN,
  color STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.template_texts definition

CREATE TABLE bronze.leaseend_db_public.template_texts (
  id INT,
  description STRING,
  text STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  key STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.temporary_infos definition

CREATE TABLE bronze.leaseend_db_public.temporary_infos (
  id STRING,
  data STRING,
  status STRING,
  deal_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  auth0_id STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  opt_event_id INT,
  search_vector STRING,
  transform_type STRING,
  affiliate_id INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.text_messages definition

CREATE TABLE bronze.leaseend_db_public.text_messages (
  id INT,
  message_id STRING,
  account_id STRING,
  user_id STRING,
  recipient_phone_number STRING,
  sender_phone_number STRING,
  message STRING,
  status STRING,
  media STRING,
  resolved_media STRING,
  date_created TIMESTAMP,
  date_updated TIMESTAMP,
  date_sent TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  media_list STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  media_list_object STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.thunder_numbers definition

CREATE TABLE bronze.leaseend_db_public.thunder_numbers (
  id INT,
  phone_number STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.titling_pods definition

CREATE TABLE bronze.leaseend_db_public.titling_pods (
  id INT,
  name STRING,
  color STRING,
  us_states STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  problem_solver BOOLEAN,
  archived BOOLEAN,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  us_states_object STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.titling_pods_users definition

CREATE TABLE bronze.leaseend_db_public.titling_pods_users (
  id INT,
  titling_pod_id INT,
  user_id STRING,
  role STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.titling_pod_archive_logs definition

CREATE TABLE bronze.leaseend_db_public.titling_pod_archive_logs (
  id INT,
  titling_pod_id INT,
  user_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.trace_error_logs definition

CREATE TABLE bronze.leaseend_db_public.trace_error_logs (
  id INT,
  trace_id STRING,
  message STRING,
  details STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.twilio_friendly_names definition

CREATE TABLE bronze.leaseend_db_public.twilio_friendly_names (
  id STRING,
  friendly_name STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.twilio_voice_recordings definition

CREATE TABLE bronze.leaseend_db_public.twilio_voice_recordings (
  id STRING,
  date_created DATE,
  date_updated DATE,
  duration INT,
  media_url STRING,
  call_sid STRING,
  transcription_url STRING,
  recording_url STRING,
  in_s3 BOOLEAN,
  in_twilio BOOLEAN,
  errored BOOLEAN,
  _fivetran_deleted BOOLEAN,
  _fivetran_id STRING,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.users definition

CREATE TABLE bronze.leaseend_db_public.users (
  id STRING,
  email STRING,
  name STRING,
  nickname STRING,
  phone_number STRING,
  twilio_number STRING,
  hubspot_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  call_status STRING,
  voice_recording_url STRING,
  deleted_at TIMESTAMP,
  can_claim_as_closer BOOLEAN,
  hours STRING,
  on_vacation BOOLEAN,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  potential_user_id INT,
  auto_assign_deals BOOLEAN,
  recruiter_id STRING,
  overnight_deals BOOLEAN,
  auth0_roles STRING,
  max_auto_assign_deals INT,
  location STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.users_active_time definition

CREATE TABLE bronze.leaseend_db_public.users_active_time (
  id INT,
  user_id STRING,
  active_at TIMESTAMP,
  inactive_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.verification_attempts definition

CREATE TABLE bronze.leaseend_db_public.verification_attempts (
  id INT,
  deal_id INT,
  auth0_id STRING,
  is_cobuyer BOOLEAN,
  attempts INT,
  expires_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.vouch_emails definition

CREATE TABLE bronze.leaseend_db_public.vouch_emails (
  id INT,
  name STRING,
  email STRING,
  trustpilot_link STRING,
  deal_id INT,
  opens INT,
  clicks INT,
  date TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.leaseend_db_public.waiting_calls definition

CREATE TABLE bronze.leaseend_db_public.waiting_calls (
  id INT,
  from STRING,
  hunt_group_slug STRING,
  deal_id INT,
  conference_id STRING,
  started_at STRING,
  deal_state STRING,
  customer_name STRING,
  to STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  processing_started_at STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  transferred_from STRING,
  deal_type STRING,
  is_thunder BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.leaseend_db_public.warranty_terms definition

CREATE TABLE bronze.leaseend_db_public.warranty_terms (
  id INT,
  make STRING,
  years INT,
  miles INT,
  as_of_date DATE,
  type STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');



  -- bronze.bamboohr.account_user definition

CREATE TABLE bronze.bamboohr.account_user (
  id INT,
  employee_id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  status STRING,
  last_login TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.bamboohr.employee definition

CREATE TABLE bronze.bamboohr.employee (
  id INT,
  original_hire_date DATE,
  last_name STRING,
  hire_date DATE,
  aca_status STRING,
  job_title STRING,
  last_changed TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  termination_date DATE,
  employment_status_date DATE,
  first_name STRING,
  reporting_to STRING,
  employment_status STRING,
  state STRING,
  department STRING,
  fte STRING,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');

  -- silver.deal.transaction_event definition

CREATE TABLE silver.deal.transaction_event (
  timestamp TIMESTAMP,
  customer_id INT,
  deal_id INT,
  event_type STRING,
  event_sub_type STRING,
  rep_id STRING,
  rep_name STRING,
  user_note STRING,
  system_note STRING,
  title STRING,
  event_source STRING,
  event_source_id STRING)
USING delta
COMMENT 'The \'transaction_event\' table contains data about events that occur during driver deals. It includes details such as the event type, subtype, representative ID, and notes from both the user and system. This data can be used to analyze customer behavior, track representative performance, and identify potential issues or opportunities in the transaction process. It can also help in understanding the source of events, enabling better management and improvement of the transaction system.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '4');



  -- bronze.hubspot.company definition

CREATE TABLE bronze.hubspot.company (
  id BIGINT,
  portal_id BIGINT,
  is_deleted BOOLEAN,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  property_num_notes DOUBLE,
  property_hubspot_team_id BIGINT,
  property_industry STRING,
  property_hs_all_accessible_team_ids BIGINT,
  property_num_associated_contacts DOUBLE,
  property_hs_analytics_first_timestamp TIMESTAMP,
  property_hs_num_contacts_with_buying_roles DOUBLE,
  property_hs_time_in_lead DOUBLE,
  property_name STRING,
  property_notes_last_updated TIMESTAMP,
  property_hs_lastmodifieddate TIMESTAMP,
  property_country STRING,
  property_hs_pipeline STRING,
  property_hs_date_entered_lead TIMESTAMP,
  property_hs_analytics_latest_source STRING,
  property_hs_analytics_latest_source_timestamp TIMESTAMP,
  property_hs_predictivecontactscore_v_2 DOUBLE,
  property_hs_object_source STRING,
  property_hs_analytics_source_data_2 STRING,
  property_num_conversion_events DOUBLE,
  property_hs_analytics_num_page_views DOUBLE,
  property_hs_analytics_source_data_1 STRING,
  property_hs_num_decision_makers DOUBLE,
  property_hs_num_open_deals DOUBLE,
  property_hs_num_child_companies DOUBLE,
  property_hs_was_imported BOOLEAN,
  property_hs_updated_by_user_id DOUBLE,
  property_hs_created_by_user_id DOUBLE,
  property_hs_num_blockers DOUBLE,
  property_lifecyclestage STRING,
  property_hs_analytics_num_visits DOUBLE,
  property_hubspot_owner_assigneddate TIMESTAMP,
  property_hs_object_source_id STRING,
  property_createdate TIMESTAMP,
  property_hs_analytics_source STRING,
  property_num_contacted_notes DOUBLE,
  property_hs_all_team_ids BIGINT,
  property_hs_object_source_label STRING,
  property_hs_target_account_probability DOUBLE,
  property_hubspot_owner_id BIGINT,
  property_hs_object_id DOUBLE,
  property_hs_analytics_latest_source_data_2 STRING,
  property_hs_user_ids_of_all_owners BIGINT,
  property_first_contact_createdate TIMESTAMP,
  property_hs_all_owner_ids BIGINT,
  property_hs_analytics_latest_source_data_1 STRING,
  property_hs_object_source_user_id DOUBLE,
  property_description STRING,
  property_linkedinbio STRING,
  property_founded_year BIGINT,
  property_timezone STRING,
  property_annualrevenue DOUBLE,
  property_city STRING,
  property_website STRING,
  property_web_technologies STRING,
  property_address STRING,
  property_linkedin_company_page STRING,
  property_phone STRING,
  property_zip STRING,
  property_state STRING,
  property_domain STRING,
  property_facebook_company_page STRING,
  property_is_public BOOLEAN,
  property_twitterhandle STRING,
  property_numberofemployees DOUBLE,
  property_total_money_raised STRING,
  property_address_2 STRING,
  property_notes_next_activity_date TIMESTAMP,
  property_hs_last_open_task_date TIMESTAMP,
  property_hs_is_target_account BOOLEAN,
  property_hs_object_source_detail_1 STRING,
  property_hs_notes_next_activity_type STRING,
  property_hs_all_assigned_business_unit_ids BIGINT,
  property_hs_merged_object_ids BIGINT,
  property_hs_logo_url STRING,
  property_hs_analytics_last_visit_timestamp TIMESTAMP,
  property_recent_conversion_event_name STRING,
  property_recent_conversion_date TIMESTAMP,
  property_hs_analytics_first_visit_timestamp TIMESTAMP,
  property_hs_analytics_last_timestamp TIMESTAMP,
  property_first_conversion_event_name STRING,
  property_first_conversion_date TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.hubspot.company_property_history definition

CREATE TABLE bronze.hubspot.company_property_history (
  company_id BIGINT,
  name STRING,
  timestamp TIMESTAMP,
  value STRING,
  source_id STRING,
  source STRING,
  _fivetran_synced TIMESTAMP,
  _fivetran_start TIMESTAMP,
  _fivetran_end TIMESTAMP,
  _fivetran_active BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.hubspot.contact definition

CREATE TABLE bronze.hubspot.contact (
  property_cold_emailed TIMESTAMP,
  property_hs_email_optout_73870428 BOOLEAN,
  property_hs_time_between_contact_creation_and_deal_close DOUBLE,
  property_hs_time_to_move_from_lead_to_customer DOUBLE,
  property_hs_is_unworked BOOLEAN,
  property_hs_first_outreach_date TIMESTAMP,
  property_hs_date_exited_opportunity TIMESTAMP,
  property_rr_memberid_1 STRING,
  property_last_observed TIMESTAMP,
  property_days_to_close DOUBLE,
  property_hs_time_in_lead DOUBLE,
  property_notes_last_updated TIMESTAMP,
  property_closedate TIMESTAMP,
  property_hs_lastmodifieddate TIMESTAMP,
  property_hs_v_2_cumulative_time_in_subscriber DOUBLE,
  property_vouch_app_property_no_vouches_submitted DOUBLE,
  property_vehicle_type STRING,
  property_hs_date_exited_customer TIMESTAMP,
  property_hs_analytics_first_referrer STRING,
  property_fuel_type STRING,
  property_when_does_your_auto_lease_end STRING,
  property_hs_email_bad_address BOOLEAN,
  property_hs_v_2_date_entered_other TIMESTAMP,
  property_hs_was_imported BOOLEAN,
  property_hs_analytics_last_referrer STRING,
  property_hs_timezone STRING,
  property_hs_v_2_latest_time_in_lead DOUBLE,
  property_hs_facebook_click_id STRING,
  property_hs_marketable_reason_type STRING,
  property_acquisition_date TIMESTAMP,
  property_hs_registered_member DOUBLE,
  property_website STRING,
  property_lease_end_customer STRING,
  property_hs_email_optout_25286659 BOOLEAN,
  property_rr_twittershare_1 STRING,
  property_first_deal_created_date TIMESTAMP,
  property_rr_source STRING,
  property_hs_date_entered_100023332 TIMESTAMP,
  property_hs_user_ids_of_all_owners BIGINT,
  property_cold_call_notes STRING,
  property_rr_dashboardlink_1 STRING,
  property_rr_memberurl STRING,
  property_jobtitle STRING,
  property_hs_date_entered_customer TIMESTAMP,
  property_rr_linkshare_1 STRING,
  property_google_ad_click_date TIMESTAMP,
  property_hs_latest_source STRING,
  property_hs_email_quarantined BOOLEAN,
  property_hs_all_accessible_team_ids BIGINT,
  property_associatedcompanyid DOUBLE,
  property_total_revenue DOUBLE,
  property_informed_delivery_click TIMESTAMP,
  property_opt_out_notes STRING,
  property_rr_lastsharedate_1 TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  property_opt_out_2 TIMESTAMP,
  property_calendly_answer_3 STRING,
  property_calendly_answer_2 STRING,
  property_calendly_answer_1 STRING,
  property_hs_v_2_cumulative_time_in_opportunity DOUBLE,
  property_suppression_code STRING,
  property_hs_analytics_first_visit_timestamp TIMESTAMP,
  property_hs_time_in_100023332 DOUBLE,
  property_hs_v_2_cumulative_time_in_lead DOUBLE,
  property_hs_v_2_latest_time_in_subscriber DOUBLE,
  property_hs_membership_has_accessed_private_content DOUBLE,
  property_num_conversion_events DOUBLE,
  property_hs_analytics_num_page_views DOUBLE,
  property_rr_totalpendingreferrals_1 DOUBLE,
  property_currentlyinworkflow BOOLEAN,
  property_traffic_medium STRING,
  property_hs_v_2_date_exited_lead TIMESTAMP,
  property_ip_state_code STRING,
  property_append_service STRING,
  property_hs_date_exited_lead TIMESTAMP,
  property_hs_created_by_user_id DOUBLE,
  property_hs_social_num_broadcast_clicks DOUBLE,
  property_rr_facebookshare_1 STRING,
  property_first_conversion_event_name STRING,
  property_hs_date_entered_opportunity TIMESTAMP,
  property_hs_email_bounce DOUBLE,
  property_hs_sales_email_last_replied TIMESTAMP,
  property_kixie_upload_date TIMESTAMP,
  property_encryption STRING,
  property_verse_unqualified STRING,
  property_hs_marketable_status BOOLEAN,
  property_hs_sa_first_engagement_descr STRING,
  property_hs_is_contact BOOLEAN,
  property_ip_country_code STRING,
  property_rr_smsshare_1 STRING,
  property_called_idaho TIMESTAMP,
  property_email_opt_out TIMESTAMP,
  property_hs_last_sales_activity_type STRING,
  property_scheduled_callback_time STRING,
  property_hs_v_2_date_exited_subscriber TIMESTAMP,
  property_hs_v_2_latest_time_in_customer DOUBLE,
  property_hs_v_2_date_entered_marketingqualifiedlead TIMESTAMP,
  property_hs_v_2_latest_time_in_100023332 DOUBLE,
  property_hs_analytics_last_visit_timestamp TIMESTAMP,
  property_rr_totalapprovedreferrals_1 DOUBLE,
  property_rr_member_phone BIGINT,
  property_phone_2 STRING,
  property_contact_deal_stage STRING,
  property_hs_time_to_first_engagement DOUBLE,
  property_hs_latest_meeting_activity TIMESTAMP,
  property_hs_v_2_cumulative_time_in_customer DOUBLE,
  property_hs_v_2_latest_time_in_124866084 DOUBLE,
  property_hs_object_source STRING,
  property_campaign STRING,
  property_city STRING,
  property_referrer STRING,
  property_hs_created_by_conversations BOOLEAN,
  property_hs_time_in_124866084 DOUBLE,
  property_hs_email_optout_35074483 BOOLEAN,
  property_rr_joineddate_1 TIMESTAMP,
  property_rr_totalpendingrewardamount_1 DOUBLE,
  property_hs_time_to_move_from_opportunity_to_customer DOUBLE,
  property_recent_deal_amount DOUBLE,
  property_calendly_question_1 STRING,
  property_calendly_question_2 STRING,
  property_hs_analytics_num_visits DOUBLE,
  property_calendly_question_3 STRING,
  property_hs_v_2_date_exited_opportunity TIMESTAMP,
  property_hs_date_entered_other TIMESTAMP,
  property_hs_analytics_source STRING,
  property_retentioned TIMESTAMP,
  property_hs_date_exited_100023332 TIMESTAMP,
  property_recent_conversion_date TIMESTAMP,
  property_hs_v_2_date_exited_100023332 TIMESTAMP,
  property_le_id STRING,
  property_hs_calculated_phone_number STRING,
  property_hs_google_click_id STRING,
  property_hs_analytics_last_touch_converting_campaign STRING,
  property_hs_analytics_first_touch_converting_campaign STRING,
  property_phone STRING,
  property_data_source STRING,
  property_num_notes DOUBLE,
  property_call_outcome STRING,
  property_idaho_call_duration DOUBLE,
  property_opted_out TIMESTAMP,
  property_hs_v_2_date_entered_subscriber TIMESTAMP,
  property_hs_analytics_average_page_views DOUBLE,
  property_hs_time_to_move_from_subscriber_to_customer DOUBLE,
  property_hs_predictivescoringtier_tmp STRING,
  property_vehicle_make_model STRING,
  property_hs_v_2_cumulative_time_in_100023332 DOUBLE,
  property_recent_deal_close_date TIMESTAMP,
  property_hs_social_facebook_clicks DOUBLE,
  property_latest_text_message STRING,
  property_mobilephone STRING,
  property_test_variant_2 STRING,
  property_hs_social_last_engagement TIMESTAMP,
  property_hs_date_entered_lead TIMESTAMP,
  property_rr_referralid STRING,
  property_hs_email_first_open_date TIMESTAMP,
  property_rr_whatsappshare_1 STRING,
  property_line_type_append_date TIMESTAMP,
  property_hs_ip_timezone STRING,
  property_hs_time_in_other DOUBLE,
  property_hs_all_assigned_business_unit_ids BIGINT,
  property_called_inbound TIMESTAMP,
  property_num_unique_conversion_events DOUBLE,
  property_mailed_by STRING,
  property_page_title STRING,
  property_reason_opted_out STRING,
  property_hs_email_quarantined_reason STRING,
  property_num_associated_deals DOUBLE,
  property_hs_quarantined_emails STRING,
  property_hs_searchable_calculated_mobile_number BIGINT,
  property_hs_searchable_calculated_phone_number BIGINT,
  property_hs_updated_by_user_id DOUBLE,
  property_gauto_aup_1003 DOUBLE,
  property_hs_email_optout BOOLEAN,
  property_vouch_app_property_first_vouch_submitted_date TIMESTAMP,
  property_test_variant STRING,
  property_hs_time_in_customer DOUBLE,
  property_hs_marketable_reason_id STRING,
  property_num_contacted_notes DOUBLE,
  property_hs_v_2_cumulative_time_in_marketingqualifiedlead DOUBLE,
  property_hs_lead_status STRING,
  property_hs_all_team_ids BIGINT,
  property_hs_email_open DOUBLE,
  property_hs_object_id DOUBLE,
  property_hs_lifecyclestage_subscriber_date TIMESTAMP,
  property_rr_programjoined_1 STRING,
  property_hs_social_twitter_clicks DOUBLE,
  property_first_conversion_date TIMESTAMP,
  property_hs_analytics_last_url STRING,
  property_rr_referralcode_1 STRING,
  property_hubspot_team_id BIGINT,
  property_hs_email_first_reply_date TIMESTAMP,
  property_hs_facebook_ad_clicked BOOLEAN,
  property_matchkey STRING,
  property_do_you_currently_have_an_auto_lease STRING,
  property_ip_city STRING,
  property_do_not_mail TIMESTAMP,
  property_hs_date_entered_124866084 TIMESTAMP,
  property_email STRING,
  property_ip_country STRING,
  property_ip_state STRING,
  property_hs_pipeline STRING,
  property_hs_email_replied DOUBLE,
  property_hs_sa_first_engagement_date TIMESTAMP,
  property_hs_lifecyclestage_opportunity_date TIMESTAMP,
  property_hs_last_sales_activity_date TIMESTAMP,
  id BIGINT,
  property_hs_email_domain STRING,
  property_hs_email_last_reply_date TIMESTAMP,
  property_make STRING,
  property_hs_email_last_send_date TIMESTAMP,
  property_rr_emailshare_1 STRING,
  property_hs_v_2_date_entered_lead TIMESTAMP,
  property_called_date TIMESTAMP,
  property_email_opt_out_date TIMESTAMP,
  property_hs_v_2_date_entered_opportunity TIMESTAMP,
  property_hs_analytics_first_url STRING,
  property_append_date TIMESTAMP,
  property_hs_email_sends_since_last_engagement DOUBLE,
  property_firstname STRING,
  property_hs_email_recipient_fatigue_recovery_time TIMESTAMP,
  property_address STRING,
  property_weekday_received_verse_sms STRING,
  property_hs_sa_first_engagement_object_type STRING,
  property_hs_sequences_actively_enrolled_count DOUBLE,
  property_hs_object_source_label STRING,
  property_hs_email_last_open_date TIMESTAMP,
  property_hs_v_2_latest_time_in_marketingqualifiedlead DOUBLE,
  property_hs_v_2_date_exited_marketingqualifiedlead TIMESTAMP,
  property_lastname STRING,
  property_hs_analytics_revenue DOUBLE,
  property_hs_email_first_click_date TIMESTAMP,
  property_hs_v_2_date_exited_124866084 TIMESTAMP,
  property_email_drop TIMESTAMP,
  property_hs_v_2_date_entered_100023332 TIMESTAMP,
  property_mailed_date TIMESTAMP,
  property_hs_email_hard_bounce_reason_enum STRING,
  property_hs_email_last_email_name STRING,
  property_rr_totalqualifiedreferrals_1 DOUBLE,
  property_hs_associated_target_accounts DOUBLE,
  property_lease_end_deal STRING,
  property_hs_latest_source_data_1 STRING,
  property_hs_latest_source_data_2 STRING,
  property_hs_analytics_last_timestamp TIMESTAMP,
  property_hs_v_2_cumulative_time_in_124866084 DOUBLE,
  property_hs_social_linkedin_clicks DOUBLE,
  property_monthly_payment STRING,
  property_hs_time_to_move_from_marketingqualifiedlead_to_customer DOUBLE,
  property_rr_totalreferralviews_1 DOUBLE,
  property_rr_member_full_name STRING,
  property_texted_date TIMESTAMP,
  property_hs_currently_enrolled_in_prospecting_agent BOOLEAN,
  property_rr_facebookmessengershare_1 STRING,
  property_auto_lease_ending_date TIMESTAMP,
  property_hs_latest_source_timestamp TIMESTAMP,
  property_lastmodifieddate TIMESTAMP,
  property_hs_email_last_click_date TIMESTAMP,
  property_lifecyclestage STRING,
  property_rr_totalissuedrewardamount_1 DOUBLE,
  property_source STRING,
  property_purchase_year BIGINT,
  property_hubspot_owner_assigneddate TIMESTAMP,
  property_hs_object_source_id STRING,
  property_hs_lifecyclestage_lead_date TIMESTAMP,
  property_createdate TIMESTAMP,
  property_hs_predictivescoringtier STRING,
  property_hubspot_owner_id BIGINT,
  property_hs_calculated_phone_number_country_code STRING,
  property_hs_time_between_contact_creation_and_deal_creation DOUBLE,
  property_rr_member_email STRING,
  property_hs_v_2_date_exited_customer TIMESTAMP,
  property_hs_date_entered_subscriber TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  property_hs_notes_next_activity_type STRING,
  property_hs_count_is_worked DOUBLE,
  property_hs_analytics_first_timestamp TIMESTAMP,
  property_hs_sales_email_last_clicked TIMESTAMP,
  property_hs_date_entered_marketingqualifiedlead TIMESTAMP,
  property_vehicle_class STRING,
  property_hs_lifecyclestage_other_date TIMESTAMP,
  property_rr_referral_source_note STRING,
  property_hs_email_delivered DOUBLE,
  property_household_duplicate STRING,
  property_hs_v_2_latest_time_in_opportunity DOUBLE,
  property_hs_last_sales_activity_timestamp TIMESTAMP,
  property_rr_totalrewards_1 DOUBLE,
  property_notes_next_activity_date TIMESTAMP,
  property_last_enrich TIMESTAMP,
  property_emailed_date TIMESTAMP,
  property_hs_social_google_plus_clicks DOUBLE,
  property_hs_count_is_unworked DOUBLE,
  property_hs_calculated_mobile_number STRING,
  property_hs_v_2_date_entered_customer TIMESTAMP,
  property_hs_time_in_marketingqualifiedlead DOUBLE,
  property_rr_referralurl STRING,
  property_hs_sales_email_last_opened TIMESTAMP,
  property_hs_predictivecontactscore_tmp DOUBLE,
  property_auto_lease_ending_month STRING,
  property_rr_passwordset_1 STRING,
  property_recent_conversion_event_name STRING,
  property_vehicle_model STRING,
  property_auto_lease_ending_date_3 TIMESTAMP,
  property_auto_lease_ending_date_2 TIMESTAMP,
  property_inbound_call_status STRING,
  property_hs_marketable_until_renewal BOOLEAN,
  property_hs_lifecyclestage_marketingqualifiedlead_date TIMESTAMP,
  property_model_year BIGINT,
  property_vehicle_make STRING,
  property_hs_v_2_date_entered_124866084 TIMESTAMP,
  property_hs_email_first_send_date TIMESTAMP,
  property_d_2_d_status STRING,
  property_finscore DOUBLE,
  property_current_estimated_equity BIGINT,
  property_zip STRING,
  property_hs_analytics_num_event_completions DOUBLE,
  property_hs_lifecyclestage_customer_date TIMESTAMP,
  property_time_received_verse_sms STRING,
  property_rr_webshare_1 STRING,
  property_state STRING,
  property_fax STRING,
  property_dnc TIMESTAMP,
  is_deleted BOOLEAN,
  property_vouch_app_property_last_vouch_submitted_date TIMESTAMP,
  property_hs_calculated_merged_vids STRING,
  property_hs_time_in_subscriber DOUBLE,
  property_hs_time_in_opportunity DOUBLE,
  property_hs_additional_emails STRING,
  property_company STRING,
  property_texted_by STRING,
  property_hs_predictivecontactscore_v_2 DOUBLE,
  property_hs_first_engagement_object_id DOUBLE,
  property_rr_totalreferrals_1 DOUBLE,
  property_hs_object_source_detail_1 STRING,
  property_auto_lease_count_at_acquisition DOUBLE,
  property_hs_analytics_source_data_2 STRING,
  property_rr_member_referral_code STRING,
  property_hs_analytics_source_data_1 STRING,
  property_hs_merged_object_ids STRING,
  property_line_type STRING,
  property_hs_date_exited_marketingqualifiedlead TIMESTAMP,
  property_hs_all_contact_vids STRING,
  property_bureau_id STRING,
  property_notes_last_contacted TIMESTAMP,
  property_cold_call_disposition STRING,
  property_emailed_by STRING,
  property_date_of_birth STRING,
  property_hs_email_click DOUBLE,
  property_vehicle_segment STRING,
  property_hs_calculated_form_submissions STRING,
  property_scheduled_call_back_date TIMESTAMP,
  property_purchase_date TIMESTAMP,
  property_hs_all_owner_ids BIGINT,
  property_landing_page_url STRING,
  property_future_marketing TIMESTAMP,
  property_hs_prospecting_agent_actively_enrolled_count DOUBLE,
  property_hs_object_source_user_id DOUBLE,
  property_hs_date_exited_subscriber TIMESTAMP,
  property_hs_full_name_or_email STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '4');


-- bronze.hubspot.contact_company definition

CREATE TABLE bronze.hubspot.contact_company (
  contact_id BIGINT,
  company_id BIGINT,
  type_id BIGINT,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.hubspot.contact_form_submission definition

CREATE TABLE bronze.hubspot.contact_form_submission (
  page_url STRING,
  page_id STRING,
  _fivetran_synced TIMESTAMP,
  conversion_id STRING,
  form_id STRING,
  portal_id BIGINT,
  contact_id BIGINT,
  title STRING,
  timestamp TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.hubspot.contact_list definition

CREATE TABLE bronze.hubspot.contact_list (
  id BIGINT,
  name STRING,
  deleteable BOOLEAN,
  dynamic BOOLEAN,
  portal_id BIGINT,
  updated_at TIMESTAMP,
  created_at TIMESTAMP,
  metadata_last_processing_state_change_at TIMESTAMP,
  metadata_processing STRING,
  metadata_last_size_change_at TIMESTAMP,
  metadata_error STRING,
  metadata_size BIGINT,
  offset BIGINT,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.hubspot.contact_list_member definition

CREATE TABLE bronze.hubspot.contact_list_member (
  contact_id BIGINT,
  contact_list_id BIGINT,
  added_at TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.hubspot.contact_property_history definition

CREATE TABLE bronze.hubspot.contact_property_history (
  contact_id BIGINT,
  name STRING,
  timestamp TIMESTAMP,
  value STRING,
  source_id STRING,
  source STRING,
  _fivetran_synced TIMESTAMP,
  _fivetran_start TIMESTAMP,
  _fivetran_end TIMESTAMP,
  _fivetran_active BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.hubspot.engagement definition

CREATE TABLE bronze.hubspot.engagement (
  id BIGINT,
  type STRING,
  portal_id BIGINT,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.hubspot.engagement_note definition

CREATE TABLE bronze.hubspot.engagement_note (
  engagement_id BIGINT,
  type STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  property_hs_body_preview_html STRING,
  property_hs_updated_by_user_id DOUBLE,
  property_hs_created_by_user_id DOUBLE,
  property_hs_engagement_source STRING,
  property_hs_body_preview STRING,
  property_hubspot_owner_assigneddate TIMESTAMP,
  property_hs_object_source_id STRING,
  property_hs_lastmodifieddate TIMESTAMP,
  property_hs_body_preview_is_truncated BOOLEAN,
  property_hs_note_body STRING,
  property_hs_created_by DOUBLE,
  property_hs_object_source_label STRING,
  property_hubspot_owner_id BIGINT,
  property_hs_timestamp TIMESTAMP,
  property_hs_object_id DOUBLE,
  property_hs_createdate TIMESTAMP,
  property_hs_user_ids_of_all_owners BIGINT,
  property_hs_all_owner_ids BIGINT,
  property_hs_modified_by DOUBLE,
  property_hs_object_source STRING,
  property_hs_object_source_user_id DOUBLE,
  property_hubspot_team_id BIGINT,
  property_hs_all_accessible_team_ids BIGINT,
  property_hs_all_team_ids BIGINT,
  property_hs_was_imported BOOLEAN,
  property_hs_at_mentioned_owner_ids BIGINT,
  property_hs_object_source_detail_1 STRING,
  property_hs_engagement_object_coordinates STRING,
  property_hs_object_coordinates STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.hubspot.engagement_task definition

CREATE TABLE bronze.hubspot.engagement_task (
  engagement_id BIGINT,
  type STRING,
  _fivetran_deleted BOOLEAN,
  _fivetran_synced TIMESTAMP,
  property_hs_date_entered_61_bafb_31_e_7_fa_46_ed_aaa_9_1322438_d_6_e_67_1866552342 TIMESTAMP,
  property_hs_task_for_object_type STRING,
  property_hs_task_reminders BIGINT,
  property_hs_task_priority STRING,
  property_hs_lastmodifieddate TIMESTAMP,
  property_hs_pipeline STRING,
  property_hs_date_exited_60_b_5_c_368_04_c_4_4_d_32_9_b_4_a_457_e_159_f_49_b_7_13292096 TIMESTAMP,
  property_hs_object_source STRING,
  property_hs_task_is_completed_email DOUBLE,
  property_hs_task_completion_date TIMESTAMP,
  property_hs_pipeline_stage STRING,
  property_hs_date_entered_60_b_5_c_368_04_c_4_4_d_32_9_b_4_a_457_e_159_f_49_b_7_13292096 TIMESTAMP,
  property_hs_task_send_default_reminder BOOLEAN,
  property_hs_task_is_completed_call DOUBLE,
  property_hs_task_missed_due_date BOOLEAN,
  property_hs_task_missed_due_date_count DOUBLE,
  property_hs_date_entered_fc_8148_fb_3_a_2_d_4_b_59_834_e_69_b_7859347_cb_1813133675 TIMESTAMP,
  property_hs_task_status STRING,
  property_hs_time_in_60_b_5_c_368_04_c_4_4_d_32_9_b_4_a_457_e_159_f_49_b_7_13292096 DOUBLE,
  property_hs_object_source_label STRING,
  property_hs_task_is_completed_sequence DOUBLE,
  property_hs_date_entered_dd_5826_e_4_c_976_4654_a_527_b_59_ada_542_e_52_2144133616 TIMESTAMP,
  property_hs_user_ids_of_all_owners BIGINT,
  property_hs_modified_by DOUBLE,
  property_hs_date_exited_af_0_e_6_a_5_c_2_ea_3_4_c_72_b_69_f_7_c_6_cb_3_fdb_591_1652950531 TIMESTAMP,
  property_hs_task_relative_reminders STRING,
  property_hs_task_type STRING,
  property_hs_date_exited_fc_8148_fb_3_a_2_d_4_b_59_834_e_69_b_7859347_cb_1813133675 TIMESTAMP,
  property_hs_body_preview_is_truncated BOOLEAN,
  property_hs_time_in_fc_8148_fb_3_a_2_d_4_b_59_834_e_69_b_7859347_cb_1813133675 DOUBLE,
  property_hs_createdate TIMESTAMP,
  property_hs_time_in_af_0_e_6_a_5_c_2_ea_3_4_c_72_b_69_f_7_c_6_cb_3_fdb_591_1652950531 DOUBLE,
  property_hs_task_is_completed DOUBLE,
  property_hs_task_is_completed_linked_in DOUBLE,
  property_hs_time_in_dd_5826_e_4_c_976_4654_a_527_b_59_ada_542_e_52_2144133616 DOUBLE,
  property_hs_task_is_all_day BOOLEAN,
  property_hs_date_exited_dd_5826_e_4_c_976_4654_a_527_b_59_ada_542_e_52_2144133616 TIMESTAMP,
  property_hs_time_in_61_bafb_31_e_7_fa_46_ed_aaa_9_1322438_d_6_e_67_1866552342 DOUBLE,
  property_hs_date_entered_af_0_e_6_a_5_c_2_ea_3_4_c_72_b_69_f_7_c_6_cb_3_fdb_591_1652950531 TIMESTAMP,
  property_hs_updated_by_user_id DOUBLE,
  property_hs_created_by_user_id DOUBLE,
  property_hs_engagement_source STRING,
  property_hubspot_owner_assigneddate TIMESTAMP,
  property_hs_object_source_id STRING,
  property_hs_task_is_overdue BOOLEAN,
  property_hs_task_family STRING,
  property_hs_created_by DOUBLE,
  property_hubspot_owner_id BIGINT,
  property_hs_timestamp TIMESTAMP,
  property_hs_object_id DOUBLE,
  property_hs_task_is_past_due_date BOOLEAN,
  property_hs_task_subject STRING,
  property_hs_all_owner_ids BIGINT,
  property_hs_object_source_user_id DOUBLE,
  property_hs_task_body STRING,
  property_hs_body_preview_html STRING,
  property_hs_body_preview STRING,
  property_hs_engagement_source_id BIGINT,
  property_hs_unique_id STRING,
  property_hs_gdpr_deleted BOOLEAN,
  property_hs_task_completion_count DOUBLE,
  property_hubspot_team_id BIGINT,
  property_hs_all_accessible_team_ids BIGINT,
  property_hs_task_last_contact_outreach TIMESTAMP,
  property_hs_at_mentioned_owner_ids BIGINT,
  property_hs_all_team_ids BIGINT,
  property_hs_task_contact_timezone STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.hubspot.line_item definition

CREATE TABLE bronze.hubspot.line_item (
  id BIGINT,
  _fivetran_deleted BOOLEAN,
  product_id BIGINT,
  property_hs_margin_acv DOUBLE,
  property_hs_sku STRING,
  property_description STRING,
  property_hs_arr DOUBLE,
  property_hs_recurring_billing_number_of_payments DOUBLE,
  property_hs_total_discount DOUBLE,
  property_hs_tcv DOUBLE,
  property_name STRING,
  property_hs_lastmodifieddate TIMESTAMP,
  property_amount DOUBLE,
  property_quantity DOUBLE,
  property_hs_margin_arr DOUBLE,
  property_hs_acv DOUBLE,
  property_hs_pre_discount_amount DOUBLE,
  property_hs_object_source STRING,
  property_hs_margin DOUBLE,
  property_price DOUBLE,
  property_hs_post_tax_amount DOUBLE,
  property_hs_margin_mrr DOUBLE,
  property_hs_margin_tcv DOUBLE,
  property_hs_mrr DOUBLE,
  property_hs_object_source_id STRING,
  property_createdate TIMESTAMP,
  property_hs_object_source_label STRING,
  property_hs_object_id DOUBLE,
  _fivetran_synced TIMESTAMP,
  property_hs_cost_of_goods_sold DOUBLE,
  property_hs_updated_by_user_id DOUBLE,
  property_hs_created_by_user_id DOUBLE,
  property_hs_object_source_user_id DOUBLE,
  property_hs_position_on_quote DOUBLE,
  property_hs_tax_amount DOUBLE)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.hubspot.line_item_deal definition

CREATE TABLE bronze.hubspot.line_item_deal (
  line_item_id BIGINT,
  deal_id BIGINT,
  type_id BIGINT,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.hubspot.line_item_property_history definition

CREATE TABLE bronze.hubspot.line_item_property_history (
  line_item_id BIGINT,
  name STRING,
  timestamp TIMESTAMP,
  value STRING,
  source_id STRING,
  source STRING,
  _fivetran_synced TIMESTAMP,
  _fivetran_start TIMESTAMP,
  _fivetran_end TIMESTAMP,
  _fivetran_active BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');