-- bronze.ns.account definition

CREATE TABLE bronze.ns.account (
  id BIGINT,
  accountsearchdisplayname STRING,
  accountsearchdisplaynamecopy STRING,
  acctnumber STRING,
  accttype STRING,
  availablebalance DOUBLE,
  balance DOUBLE,
  billableexpensesacct BIGINT,
  category1099misc BIGINT,
  class BIGINT,
  deferralacct BIGINT,
  department BIGINT,
  description STRING,
  displaynamewithhierarchy STRING,
  eliminate STRING,
  externalid STRING,
  fullname STRING,
  includechildren STRING,
  inventory STRING,
  isinactive STRING,
  issummary STRING,
  lastmodifieddate TIMESTAMP,
  location BIGINT,
  parent BIGINT,
  reconcilewithmatching STRING,
  revalue STRING,
  sbankname STRING,
  sbankroutingnumber STRING,
  sspecacct STRING,
  subsidiary STRING,
  unit BIGINT,
  unitstype BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_flux_acc_amt DOUBLE,
  custrecord_nact_flux_acc_perc DOUBLE,
  custrecord_nact_account_flux_assignee BIGINT,
  custrecord_nact_account_recon_rule BIGINT,
  custrecord_nact_flux_acc_gen STRING,
  custrecord_nact_flux_is_consolidated STRING,
  custrecord_nact_flux_account_abs_date BIGINT,
  custrecord_nact_account_flux_approvers STRING,
  custrecord_nact_account_flux_group BIGINT,
  custrecord_nact_flux_account_rel_date BIGINT,
  custrecord_tax1099nec BIGINT,
  custrecord_tax1099misc BIGINT,
  custrecord_nact_account_recon_assignee BIGINT,
  custrecord_nact_account_recon_reviewer STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.accountingbook definition

CREATE TABLE bronze.ns.accountingbook (
  id BIGINT,
  basebook BIGINT,
  effectiveperiod BIGINT,
  externalid STRING,
  isadjustmentonly STRING,
  isconsolidated STRING,
  isprimary STRING,
  lastmodifieddate TIMESTAMP,
  name STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  includechildren STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.accountingbooksubsidiaries definition

CREATE TABLE bronze.ns.accountingbooksubsidiaries (
  _fivetran_id STRING,
  accountingbook BIGINT,
  status STRING,
  subsidiary BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.accountingperiod definition

CREATE TABLE bronze.ns.accountingperiod (
  id BIGINT,
  alllocked STRING,
  allownonglchanges STRING,
  aplocked STRING,
  arlocked STRING,
  closed STRING,
  closedondate TIMESTAMP,
  enddate TIMESTAMP,
  isadjust STRING,
  isinactive STRING,
  isposting STRING,
  isquarter STRING,
  isyear STRING,
  lastmodifieddate TIMESTAMP,
  parent BIGINT,
  payrolllocked STRING,
  periodname STRING,
  startdate TIMESTAMP,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  year BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.accountingperiodfiscalcalendars definition

CREATE TABLE bronze.ns.accountingperiodfiscalcalendars (
  _fivetran_id STRING,
  accountingperiod BIGINT,
  fiscalcalendar BIGINT,
  fullname STRING,
  parent BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.accounttype definition

CREATE TABLE bronze.ns.accounttype (
  id STRING,
  balancesheet STRING,
  defaultcashflowratetype STRING,
  defaultgeneralratetype STRING,
  eliminationalgo STRING,
  includeinrevaldefault STRING,
  internalid BIGINT,
  left STRING,
  longname STRING,
  seqnum BIGINT,
  usercanchangerevaloption STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.classification definition

CREATE TABLE bronze.ns.classification (
  externalid STRING,
  fullname STRING,
  id BIGINT,
  includechildren STRING,
  isinactive STRING,
  lastmodifieddate TIMESTAMP,
  name STRING,
  parent BIGINT,
  subsidiary STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.contactnote definition

CREATE TABLE bronze.ns.contactnote (
  entity BIGINT,
  id BIGINT,
  author BIGINT,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notedate TIMESTAMP,
  notetype BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  time TIMESTAMP)
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


-- bronze.ns.currency definition

CREATE TABLE bronze.ns.currency (
  id BIGINT,
  currencyprecision BIGINT,
  displaysymbol STRING,
  exchangerate DOUBLE,
  externalid STRING,
  isbasecurrency STRING,
  isinactive STRING,
  lastmodifieddate TIMESTAMP,
  name STRING,
  overridecurrencyformat STRING,
  symbol STRING,
  symbolplacement BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.customer definition

CREATE TABLE bronze.ns.customer (
  id BIGINT,
  accountnumber STRING,
  alcoholrecipienttype STRING,
  altemail STRING,
  altname STRING,
  altphone STRING,
  balancesearch DOUBLE,
  billingratecard BIGINT,
  buyingreason BIGINT,
  buyingtimeframe BIGINT,
  campaignevent BIGINT,
  category BIGINT,
  comments STRING,
  companyname STRING,
  consolbalancesearch DOUBLE,
  consoldaysoverduesearch BIGINT,
  consoloverduebalancesearch DOUBLE,
  consolunbilledorderssearch DOUBLE,
  contact BIGINT,
  contactlist STRING,
  creditholdoverride STRING,
  creditlimit DOUBLE,
  currency BIGINT,
  custentity_2663_customer_refund STRING,
  custentity_2663_direct_debit STRING,
  custentity_2663_email_address_notif STRING,
  custentity_9572_custref_file_format BIGINT,
  custentity_9572_dd_file_format BIGINT,
  custentity_9572_ddcust_entitybank_sub BIGINT,
  custentity_9572_ddcust_entitybnkformat BIGINT,
  custentity_9572_refcust_entitybnkformat BIGINT,
  custentity_9572_refundcust_entitybnk_sub BIGINT,
  custentity_9997_dd_file_format BIGINT,
  custentity_co_signer STRING,
  custentity_date_lsa TIMESTAMP,
  custentity_esc_annual_revenue DOUBLE,
  custentity_esc_industry BIGINT,
  custentity_esc_no_of_employees BIGINT,
  custentity_link_lsa STRING,
  custentity_link_name_lsa STRING,
  custentity_naw_trans_need_approval STRING,
  dateclosed TIMESTAMP,
  datecreated TIMESTAMP,
  defaultbankaccount BIGINT,
  defaultbillingaddress BIGINT,
  defaultorderpriority DOUBLE,
  defaultshippingaddress BIGINT,
  displaysymbol STRING,
  duplicate STRING,
  email STRING,
  emailpreference STRING,
  emailtransactions STRING,
  enddate TIMESTAMP,
  entityid STRING,
  entitynumber BIGINT,
  entitystatus BIGINT,
  entitytitle STRING,
  estimatedbudget DOUBLE,
  externalid STRING,
  fax STRING,
  faxtransactions STRING,
  firstname STRING,
  firstorderdate TIMESTAMP,
  firstsaledate TIMESTAMP,
  globalsubscriptionstatus BIGINT,
  homephone STRING,
  isautogeneratedrepresentingentity STRING,
  isbudgetapproved STRING,
  isinactive STRING,
  isperson STRING,
  language STRING,
  lastmodifieddate TIMESTAMP,
  lastname STRING,
  lastorderdate TIMESTAMP,
  lastsaledate TIMESTAMP,
  leadsource BIGINT,
  middlename STRING,
  mobilephone STRING,
  negativenumberformat BIGINT,
  numberformat BIGINT,
  oncredithold STRING,
  overduebalancesearch DOUBLE,
  overridecurrencyformat STRING,
  parent BIGINT,
  phone STRING,
  prefccprocessor BIGINT,
  pricelevel BIGINT,
  printoncheckas STRING,
  printtransactions STRING,
  probability DOUBLE,
  receivablesaccount BIGINT,
  reminderdays BIGINT,
  representingsubsidiary BIGINT,
  resalenumber STRING,
  salesreadiness BIGINT,
  salesrep BIGINT,
  salutation STRING,
  searchstage STRING,
  shipcomplete STRING,
  shippingcarrier STRING,
  shippingitem BIGINT,
  startdate TIMESTAMP,
  symbolplacement BIGINT,
  terms BIGINT,
  territory BIGINT,
  thirdpartyacct STRING,
  thirdpartycarrier STRING,
  thirdpartycountry STRING,
  thirdpartyzipcode STRING,
  title STRING,
  unbilledorderssearch DOUBLE,
  url STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  fullname STRING,
  isjob STRING,
  unsubscribe STRING,
  isexplicitconversion STRING,
  toplevelparent BIGINT,
  convertedtocontact BIGINT,
  lastsaleperiod BIGINT,
  firstsaleperiod BIGINT,
  convertedtocustomer BIGINT,
  dateprospect TIMESTAMP,
  datelead TIMESTAMP,
  dategrosslead TIMESTAMP,
  dateconversion TIMESTAMP,
  daysoverduesearch BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.customernote definition

CREATE TABLE bronze.ns.customernote (
  entity BIGINT,
  id BIGINT,
  author BIGINT,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notedate TIMESTAMP,
  notetype BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  time TIMESTAMP)
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


-- bronze.ns.customlist1248 definition

CREATE TABLE bronze.ns.customlist1248 (
  id BIGINT,
  created TIMESTAMP,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  name STRING,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.customlist1249 definition

CREATE TABLE bronze.ns.customlist1249 (
  id BIGINT,
  created TIMESTAMP,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  name STRING,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.customlist_nact_task_holiday_pref definition

CREATE TABLE bronze.ns.customlist_nact_task_holiday_pref (
  id BIGINT,
  created TIMESTAMP,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  name STRING,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.customlist_vb_memo_list definition

CREATE TABLE bronze.ns.customlist_vb_memo_list (
  id BIGINT,
  externalid STRING,
  isinactive STRING,
  name STRING,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  created TIMESTAMP,
  lastmodified TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_le_variance_report_memos definition

CREATE TABLE bronze.ns.customrecord_le_variance_report_memos (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_accrual definition

CREATE TABLE bronze.ns.customrecord_nact_accrual (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_accrual_account BIGINT,
  custrecord_nact_accrual_act_reversal_dat TIMESTAMP,
  custrecord_nact_accrual_adjusted_amount DOUBLE,
  custrecord_nact_accrual_amount DOUBLE,
  custrecord_nact_accrual_auto_rev_date TIMESTAMP,
  custrecord_nact_accrual_class BIGINT,
  custrecord_nact_accrual_create_from_tran BIGINT,
  custrecord_nact_accrual_created_from_key STRING,
  custrecord_nact_accrual_currency BIGINT,
  custrecord_nact_accrual_current_amount DOUBLE,
  custrecord_nact_accrual_date TIMESTAMP,
  custrecord_nact_accrual_department BIGINT,
  custrecord_nact_accrual_desc STRING,
  custrecord_nact_accrual_execution_log STRING,
  custrecord_nact_accrual_expense_account BIGINT,
  custrecord_nact_accrual_from_automation BIGINT,
  custrecord_nact_accrual_journal_entry BIGINT,
  custrecord_nact_accrual_location BIGINT,
  custrecord_nact_accrual_message_json STRING,
  custrecord_nact_accrual_posted_amount DOUBLE,
  custrecord_nact_accrual_reversal BIGINT,
  custrecord_nact_accrual_status BIGINT,
  custrecord_nact_accrual_subsidiary BIGINT,
  custrecord_nact_accrual_task_key STRING,
  custrecord_nact_accrual_type BIGINT,
  custrecord_nact_accrual_vendor BIGINT,
  custrecord_nact_amort_accrual_alloc_temp BIGINT,
  custrecord_nact_amort_expense_alloc_temp BIGINT,
  custrecord_nact_memo STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_accrual_automation definition

CREATE TABLE bronze.ns.customrecord_nact_accrual_automation (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_acc_auto_accrual_type BIGINT,
  custrecord_nact_acc_auto_column_label STRING,
  custrecord_nact_acc_auto_create_type BIGINT,
  custrecord_nact_acc_auto_date_field_id STRING,
  custrecord_nact_acc_auto_default_vendor BIGINT,
  custrecord_nact_acc_auto_relative_date BIGINT,
  custrecord_nact_acc_auto_saved_search BIGINT,
  custrecord_nact_acc_auto_sub_label STRING,
  custrecord_nact_acc_auto_vendor_label STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_accrual_modification definition

CREATE TABLE bronze.ns.customrecord_nact_accrual_modification (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_accrual_mod_accrual BIGINT,
  custrecord_nact_accrual_mod_amount DOUBLE,
  custrecord_nact_accrual_mod_date TIMESTAMP,
  custrecord_nact_accrual_mod_execute_log STRING,
  custrecord_nact_accrual_mod_journal BIGINT,
  custrecord_nact_accrual_mod_memo STRING,
  custrecord_nact_accrual_mod_message STRING,
  custrecord_nact_accrual_mod_new_amount DOUBLE,
  custrecord_nact_accrual_mod_status BIGINT,
  custrecord_nact_accrual_mod_task_key STRING,
  custrecord_nact_accrual_mod_total_accr DOUBLE,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_accrualtype definition

CREATE TABLE bronze.ns.customrecord_nact_accrualtype (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_accrual_reversal_type STRING,
  custrecord_nact_accrualtype_account BIGINT,
  custrecord_nact_accrualtype_exp_account BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_amort definition

CREATE TABLE bronze.ns.customrecord_nact_amort (
  abbreviation STRING,
  altname STRING,
  created TIMESTAMP,
  custrecord_nact_amort_amount DOUBLE,
  custrecord_nact_amort_class BIGINT,
  custrecord_nact_amort_clearing_acct_over BIGINT,
  custrecord_nact_amort_currency BIGINT,
  custrecord_nact_amort_date_capitalize TIMESTAMP,
  custrecord_nact_amort_dept BIGINT,
  custrecord_nact_amort_description STRING,
  custrecord_nact_amort_end TIMESTAMP,
  custrecord_nact_amort_flaggedforupdate STRING,
  custrecord_nact_amort_is_schedule_gen STRING,
  custrecord_nact_amort_journal STRING,
  custrecord_nact_amort_location BIGINT,
  custrecord_nact_amort_message_json STRING,
  custrecord_nact_amort_nogl STRING,
  custrecord_nact_amort_parent BIGINT,
  custrecord_nact_amort_ploverride BIGINT,
  custrecord_nact_amort_remaining_balance DOUBLE,
  custrecord_nact_amort_scriptlog STRING,
  custrecord_nact_amort_source_lines STRING,
  custrecord_nact_amort_start TIMESTAMP,
  custrecord_nact_amort_status BIGINT,
  custrecord_nact_amort_subsidiary BIGINT,
  custrecord_nact_amort_task_key STRING,
  custrecord_nact_amort_term DOUBLE,
  custrecord_nact_amort_vendor BIGINT,
  custrecord_nact_amortization_source BIGINT,
  custrecord_nact_amortization_type BIGINT,
  custrecord_nact_balance_sheet_temp BIGINT,
  custrecord_nact_income_statement_temp BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_amort_mod definition

CREATE TABLE bronze.ns.customrecord_nact_amort_mod (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_amort_mod_amort BIGINT,
  custrecord_nact_amort_mod_amount DOUBLE,
  custrecord_nact_amort_mod_current_term BIGINT,
  custrecord_nact_amort_mod_display_sched BIGINT,
  custrecord_nact_amort_mod_eff_acc_period BIGINT,
  custrecord_nact_amort_mod_eff_per_num DOUBLE,
  custrecord_nact_amort_mod_eft_date TIMESTAMP,
  custrecord_nact_amort_mod_end TIMESTAMP,
  custrecord_nact_amort_mod_end_date TIMESTAMP,
  custrecord_nact_amort_mod_ex_log STRING,
  custrecord_nact_amort_mod_go_forwrd_line BIGINT,
  custrecord_nact_amort_mod_journal BIGINT,
  custrecord_nact_amort_mod_json_message STRING,
  custrecord_nact_amort_mod_line BIGINT,
  custrecord_nact_amort_mod_rem_term BIGINT,
  custrecord_nact_amort_mod_start TIMESTAMP,
  custrecord_nact_amort_mod_status BIGINT,
  custrecord_nact_amort_mod_task_key STRING,
  custrecord_nact_amort_mod_term BIGINT,
  custrecord_nact_amort_mod_type BIGINT,
  custrecord_nact_amort_mod_value DOUBLE,
  custrecord_nact_amort_new_balance DOUBLE,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_amort_sl definition

CREATE TABLE bronze.ns.customrecord_nact_amort_sl (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_amort_sl_amort BIGINT,
  custrecord_nact_amort_sl_execution_log STRING,
  custrecord_nact_amort_sl_is_processed STRING,
  custrecord_nact_amort_sl_je BIGINT,
  custrecord_nact_amort_sl_lt_amount DOUBLE,
  custrecord_nact_amort_sl_lt_reclass_je BIGINT,
  custrecord_nact_amort_sl_modification BIGINT,
  custrecord_nact_amort_sl_period_num DOUBLE,
  custrecord_nact_amort_sl_remain_bal DOUBLE,
  custrecord_nact_amort_sl_remain_per BIGINT,
  custrecord_nact_amort_sl_start_date TIMESTAMP,
  custrecord_nact_amort_sl_task_key STRING,
  custrecord_nact_amort_sl_trans_type BIGINT,
  custrecord_nact_amortization_line_amount DOUBLE,
  custrecord_nact_amortization_line_end TIMESTAMP,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_amort_type definition

CREATE TABLE bronze.ns.customrecord_nact_amort_type (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_amort_type_bs BIGINT,
  custrecord_nact_amort_type_clearing_acct BIGINT,
  custrecord_nact_amort_type_default_term BIGINT,
  custrecord_nact_amort_type_is BIGINT,
  custrecord_nact_expense_convention BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_bank definition

CREATE TABLE bronze.ns.customrecord_nact_bank (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_bank_account_id STRING,
  custrecord_nact_bank_cursor STRING,
  custrecord_nact_bank_error_log STRING,
  custrecord_nact_bank_historical_plaid_id STRING,
  custrecord_nact_bank_id STRING,
  custrecord_nact_bank_raw STRING,
  custrecord_nact_bank_token STRING,
  custrecord_nact_error_log_users STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_bank_provider STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_bank_rule definition

CREATE TABLE bronze.ns.customrecord_nact_bank_rule (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_bank_rule_account BIGINT,
  custrecord_nact_bank_rule_bacategory STRING,
  custrecord_nact_bank_rule_date_tolerance BIGINT,
  custrecord_nact_bank_rule_flow_type BIGINT,
  custrecord_nact_bank_rule_is_confirm STRING,
  custrecord_nact_bank_rule_is_req_all STRING,
  custrecord_nact_bank_rule_limit DOUBLE,
  custrecord_nact_bank_rule_tran_type STRING,
  custrecord_nact_bank_rule_type BIGINT,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecordnact_bank_rule_bac_p_fin_categ STRING,
  custrecord_nact_bank_rule_offset_class BIGINT,
  custrecord_nact_bank_rule_bank_dept BIGINT,
  custrecord_nact_bank_rule_filter_b_acct STRING,
  custrecord_nact_bank_rule_bank_class BIGINT,
  custrecord_nact_bank_rule_bank_location BIGINT,
  custrecord_nact_bank_rule_offset_dept BIGINT,
  custrecord_nact_bank_rule_offset_locatio BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_bank_rule_match definition

CREATE TABLE bronze.ns.customrecord_nact_bank_rule_match (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_bank_rule_bank_texty_typ BIGINT,
  custrecord_nact_bank_rule_compar_typ_tra BIGINT,
  custrecord_nact_bank_rule_compsrison_typ BIGINT,
  custrecord_nact_bank_rule_criteria_rule BIGINT,
  custrecord_nact_bank_rule_match_ba_field STRING,
  custrecord_nact_bank_rule_match_tr_field STRING,
  custrecord_nact_bank_rule_text_compare STRING,
  custrecord_nact_bank_rule_text_tran STRING,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_bank_rule_is_match_null STRING,
  custrecord_nact_inf_fld_comp_type BIGINT,
  custrecord_nact_inf_acc_fld STRING,
  custrecord_nact_inf_txt_comp STRING,
  custrecord_nact_outf_txt_comp STRING,
  custrecord_nact_inf_acc_currency STRING,
  custrecord_nact_outf_acc_fld STRING,
  custrecord_nact_outf_acc_currency STRING,
  custrecord_nact_outf_fld_comp_type BIGINT,
  custrecord_nact_outf_bank_account STRING,
  custrecord_nact_inf_bank_account STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_bankacct definition

CREATE TABLE bronze.ns.customrecord_nact_bankacct (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_bankacct_bank BIGINT,
  custrecord_nact_bankacct_class BIGINT,
  custrecord_nact_bankacct_class_create BIGINT,
  custrecord_nact_bankacct_currency STRING,
  custrecord_nact_bankacct_current_balance DOUBLE,
  custrecord_nact_bankacct_cutover_date TIMESTAMP,
  custrecord_nact_bankacct_department BIGINT,
  custrecord_nact_bankacct_department_crea BIGINT,
  custrecord_nact_bankacct_gl_account BIGINT,
  custrecord_nact_bankacct_gl_offset_acct BIGINT,
  custrecord_nact_bankacct_gl_round_acct BIGINT,
  custrecord_nact_bankacct_historical_ids STRING,
  custrecord_nact_bankacct_initial_balance DOUBLE,
  custrecord_nact_bankacct_location BIGINT,
  custrecord_nact_bankacct_location_create BIGINT,
  custrecord_nact_bankacct_mask STRING,
  custrecord_nact_bankacct_official_name STRING,
  custrecord_nact_bankacct_plaid_id STRING,
  custrecord_nact_bankacct_raw STRING,
  custrecord_nact_bankacct_short_id STRING,
  custrecord_nact_bankacct_short_name STRING,
  custrecord_nact_bankacct_sub_type STRING,
  custrecord_nact_bankacct_type STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_bankacct_seg_dep STRING,
  custrecord_nact_bankacct_seg_loc STRING,
  custrecord_nact_bankacct_owner BIGINT,
  custrecord_nact_bankacct_seg_class STRING,
  custrecord_nact_bankacct_recon_gen_date BIGINT,
  custrecord_nact_bankacct_id STRING,
  custrecord_nact_bankacct_approver STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_bankmatch definition

CREATE TABLE bronze.ns.customrecord_nact_bankmatch (
  abbreviation STRING,
  created TIMESTAMP,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_bankmatchtran definition

CREATE TABLE bronze.ns.customrecord_nact_bankmatchtran (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_bankmatchtran_bank_activ BIGINT,
  custrecord_nact_bankmatchtran_group BIGINT,
  custrecord_nact_bankmatchtran_is_auto STRING,
  custrecord_nact_bankmatchtran_is_auto_rv STRING,
  custrecord_nact_bankmatchtran_lineunique STRING,
  custrecord_nact_bankmatchtran_reason STRING,
  custrecord_nact_bankmatchtran_rule BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_banktran definition

CREATE TABLE bronze.ns.customrecord_nact_banktran (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_banktran_account_id BIGINT,
  custrecord_nact_banktran_account_id_orig STRING,
  custrecord_nact_banktran_account_name STRING,
  custrecord_nact_banktran_amount DOUBLE,
  custrecord_nact_banktran_authorized_date TIMESTAMP,
  custrecord_nact_banktran_bank BIGINT,
  custrecord_nact_banktran_category STRING,
  custrecord_nact_banktran_category_primar STRING,
  custrecord_nact_banktran_check_number STRING,
  custrecord_nact_banktran_credit DOUBLE,
  custrecord_nact_banktran_date TIMESTAMP,
  custrecord_nact_banktran_debit DOUBLE,
  custrecord_nact_banktran_description STRING,
  custrecord_nact_banktran_group BIGINT,
  custrecord_nact_banktran_is_pending STRING,
  custrecord_nact_banktran_iso_currency STRING,
  custrecord_nact_banktran_merchant STRING,
  custrecord_nact_banktran_payment_channel STRING,
  custrecord_nact_banktran_person_category STRING,
  custrecord_nact_banktran_raw STRING,
  custrecord_nact_banktran_transaction_id STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_close_control definition

CREATE TABLE bronze.ns.customrecord_nact_close_control (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_close_control_account STRING,
  custrecord_nact_close_control_classes STRING,
  custrecord_nact_close_control_department STRING,
  custrecord_nact_close_control_is_locked STRING,
  custrecord_nact_close_control_locations STRING,
  custrecord_nact_close_control_period BIGINT,
  custrecord_nact_close_control_roles STRING,
  custrecord_nact_close_control_subsidiary BIGINT,
  custrecord_nact_close_control_tran_text STRING,
  custrecord_nact_close_control_trans STRING,
  custrecord_nact_close_control_users STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_comment definition

CREATE TABLE bronze.ns.customrecord_nact_comment (
  abbreviation STRING,
  altname STRING,
  created TIMESTAMP,
  custrecord_nact_comment_mentions STRING,
  custrecord_nact_comment_message STRING,
  custrecord_nact_comment_reconciliation BIGINT,
  custrecord_nact_comment_task BIGINT,
  custrecord_nact_message_json STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_dependent_task definition

CREATE TABLE bronze.ns.customrecord_nact_dependent_task (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_deptask_dep_task BIGINT,
  custrecord_nact_deptask_dep_task_status BIGINT,
  custrecord_nact_deptask_prevent_status BIGINT,
  custrecord_nact_deptask_task BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_email_automation definition

CREATE TABLE bronze.ns.customrecord_nact_email_automation (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_email_bcc_email_list STRING,
  custrecord_nact_email_bcc_netclose_users STRING,
  custrecord_nact_email_body STRING,
  custrecord_nact_email_cc_email_list STRING,
  custrecord_nact_email_cc_netclose_users STRING,
  custrecord_nact_email_from BIGINT,
  custrecord_nact_email_relative_send_day BIGINT,
  custrecord_nact_email_search_attachment BIGINT,
  custrecord_nact_email_send_on_trigger BIGINT,
  custrecord_nact_email_subject STRING,
  custrecord_nact_email_task STRING,
  custrecord_nact_email_to_address_list STRING,
  custrecord_nact_email_to_netclose_users STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_flux_anlsys definition

CREATE TABLE bronze.ns.customrecord_nact_flux_anlsys (
  abbreviation STRING,
  altname STRING,
  created TIMESTAMP,
  custrecord_nact_flux_ai_explanation STRING,
  custrecord_nact_flux_anlsys_account BIGINT,
  custrecord_nact_flux_anlsys_account_type BIGINT,
  custrecord_nact_flux_anlsys_ai_context STRING,
  custrecord_nact_flux_anlsys_alt_exp_1 STRING,
  custrecord_nact_flux_anlsys_alt_exp_2 STRING,
  custrecord_nact_flux_anlsys_alt_exp_3 STRING,
  custrecord_nact_flux_anlsys_assignee BIGINT,
  custrecord_nact_flux_anlsys_bal_refresh TIMESTAMP,
  custrecord_nact_flux_anlsys_balance_comp DOUBLE,
  custrecord_nact_flux_anlsys_balance_curr DOUBLE,
  custrecord_nact_flux_anlsys_budget_amt DOUBLE,
  custrecord_nact_flux_anlsys_chg_amt DOUBLE,
  custrecord_nact_flux_anlsys_chg_perc DOUBLE,
  custrecord_nact_flux_anlsys_comp_budget STRING,
  custrecord_nact_flux_anlsys_cons_acct_id STRING,
  custrecord_nact_flux_anlsys_currency BIGINT,
  custrecord_nact_flux_anlsys_date_com_end TIMESTAMP,
  custrecord_nact_flux_anlsys_date_com_st TIMESTAMP,
  custrecord_nact_flux_anlsys_date_cur_end TIMESTAMP,
  custrecord_nact_flux_anlsys_date_cur_st TIMESTAMP,
  custrecord_nact_flux_anlsys_exp_req STRING,
  custrecord_nact_flux_anlsys_exp_still STRING,
  custrecord_nact_flux_anlsys_explanation STRING,
  custrecord_nact_flux_anlsys_is_bal_chngd STRING,
  custrecord_nact_flux_anlsys_source BIGINT,
  custrecord_nact_flux_anlsys_sub_id STRING,
  custrecord_nact_flux_anlsys_sub_text STRING,
  custrecord_nact_flux_anlsys_task_key STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_flux_group definition

CREATE TABLE bronze.ns.customrecord_nact_flux_group (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_flux_group_abs_date BIGINT,
  custrecord_nact_flux_group_approvers STRING,
  custrecord_nact_flux_group_assignee BIGINT,
  custrecord_nact_flux_group_flux_type BIGINT,
  custrecord_nact_flux_group_norm_balance BIGINT,
  custrecord_nact_flux_group_ovr_amt DOUBLE,
  custrecord_nact_flux_group_ovr_perc DOUBLE,
  custrecord_nact_flux_group_rel_date BIGINT,
  custrecord_nact_flux_group_sort_order DOUBLE,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.customrecord_nact_flux_scenario definition

CREATE TABLE bronze.ns.customrecord_nact_flux_scenario (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_flux_sc_accounts BIGINT,
  custrecord_nact_flux_sc_ai_context STRING,
  custrecord_nact_flux_sc_budget STRING,
  custrecord_nact_flux_sc_comp_budget STRING,
  custrecord_nact_flux_sc_comp_end TIMESTAMP,
  custrecord_nact_flux_sc_comp_st TIMESTAMP,
  custrecord_nact_flux_sc_compare_period BIGINT,
  custrecord_nact_flux_sc_cur_end TIMESTAMP,
  custrecord_nact_flux_sc_cur_st TIMESTAMP,
  custrecord_nact_flux_sc_current_period BIGINT,
  custrecord_nact_flux_sc_is_acct_type_onl STRING,
  custrecord_nact_flux_sc_is_always_ai_exp STRING,
  custrecord_nact_flux_sc_is_bal_sheet_or STRING,
  custrecord_nact_flux_sc_is_complete STRING,
  custrecord_nact_flux_sc_is_copy_ai_expl STRING,
  custrecord_nact_flux_sc_is_inc_state_or STRING,
  custrecord_nact_flux_sc_large_trx_thresh DOUBLE,
  custrecord_nact_flux_sc_notes STRING,
  custrecord_nact_flux_sc_refresh_date TIMESTAMP,
  custrecord_nact_flux_sc_rel_compare_per BIGINT,
  custrecord_nact_flux_sc_rel_current_per BIGINT,
  custrecord_nact_flux_sc_source BIGINT,
  custrecord_nact_flux_sc_sub_id STRING,
  custrecord_nact_flux_sc_sub_text STRING,
  custrecord_nact_flux_sc_template STRING,
  custrecord_nact_flux_sc_thresh_bs_amt DOUBLE,
  custrecord_nact_flux_sc_thresh_bs_perc DOUBLE,
  custrecord_nact_flux_sc_thresh_is_amt DOUBLE,
  custrecord_nact_flux_sc_thresh_is_perc DOUBLE,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_flux_sc_add_seg_type STRING,
  custrecord_nact_flux_sc_include_none_seg STRING,
  custrecord_nact_flux_sc_add_seg STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_global_settings definition

CREATE TABLE bronze.ns.customrecord_nact_global_settings (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_gbl_ai_explanation_lengt BIGINT,
  custrecord_nact_gbl_ai_tran_line_limit BIGINT,
  custrecord_nact_gbl_alt_flux_exp_1_label STRING,
  custrecord_nact_gbl_alt_flux_exp_2_label STRING,
  custrecord_nact_gbl_alt_flux_exp_3_label STRING,
  custrecord_nact_gbl_auto_post STRING,
  custrecord_nact_gbl_auto_reconcile_creat STRING,
  custrecord_nact_gbl_completion_date_hold BIGINT,
  custrecord_nact_gbl_date_last_synced TIMESTAMP,
  custrecord_nact_gbl_flux_end_month BIGINT,
  custrecord_nact_gbl_flux_end_month_rfrsh BIGINT,
  custrecord_nact_gbl_is_advanced_flux_ai STRING,
  custrecord_nact_gbl_is_enabled_accrual STRING,
  custrecord_nact_gbl_is_enabled_alloc STRING,
  custrecord_nact_gbl_is_enabled_amort STRING,
  custrecord_nact_gbl_is_enabled_bank_recs STRING,
  custrecord_nact_gbl_is_enabled_close_con STRING,
  custrecord_nact_gbl_is_enabled_flux STRING,
  custrecord_nact_gbl_is_enabled_recon STRING,
  custrecord_nact_gbl_is_enabled_task_temp STRING,
  custrecord_nact_gbl_is_enabled_tasks STRING,
  custrecord_nact_gbl_is_flux_ai STRING,
  custrecord_nact_gbl_is_flux_delete_buttn STRING,
  custrecord_nact_gbl_is_id_preferred STRING,
  custrecord_nact_gbl_is_not_include_memo STRING,
  custrecord_nact_gbl_is_recon_tran_list_v STRING,
  custrecord_nact_gbl_is_req_group_on_acct STRING,
  custrecord_nact_gbl_is_task_email_triggr STRING,
  custrecord_nact_gbl_license_status BIGINT,
  custrecord_nact_gbl_lock_on_create STRING,
  custrecord_nact_gbl_lock_on_exec_type STRING,
  custrecord_nact_gbl_period_preference BIGINT,
  custrecord_nact_gbl_recon_update_balance BIGINT,
  custrecord_nact_gbl_tier BIGINT,
  custrecord_nact_gbl_transition_date TIMESTAMP,
  custrecord_nact_gbl_user_limit BIGINT,
  custrecord_nact_gbl_user_limit_tasks BIGINT,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_gbl_finicity_customer_id STRING,
  custrecord_nact_gbl_skript_consumer_id STRING,
  custrecord_nact_gbl_aus_business_number STRING,
  custrecord_nact_gbl_skript_client_id STRING,
  custrecord_nact_setup_payables_account BIGINT,
  custrecord_nact_gbl_skript_client_secret STRING,
  custrecord_nact_setup_receivables_accoun BIGINT,
  custrecord_nact_gbl_enable_cc_customer STRING,
  custrecord_nact_gbl_enable_cc_acct_book STRING,
  custrecord_nact_gbl_enable_cc_vendor STRING,
  custrecord_nact_gbl_gen_recon_chunks BIGINT,
  custrecord_nact_gbl_recon_item_last_sync TIMESTAMP,
  custrecord_nact_gbl_amort_start_before STRING,
  custrecord_nact_gbl_flux_approvals STRING,
  custrecord_nact_gbl_recon_show_assig_ico STRING,
  custrecord_nact_is_accrual_auto_approve STRING,
  custrecord_nact_gbl_ic_customer_alias STRING,
  custrecord_nact_gbl_show_assigee_icon STRING,
  custrecord_nact_gbl_req_task_start STRING,
  custrecord_nact_gbl_limited_tasks STRING,
  custrecord_nact_gbl_task_app_templates STRING,
  custrecord_nact_gbl_is_recon_multi_book STRING,
  custrecord_nact_gbl_ic_vendor_alias STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_non_working_days definition

CREATE TABLE bronze.ns.customrecord_nact_non_working_days (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_non_working_days_date TIMESTAMP,
  custrecord_nact_non_working_days_wrk_cal BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_recon definition

CREATE TABLE bronze.ns.customrecord_nact_recon (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_recon_accounting_period BIGINT,
  custrecord_nact_recon_accounts STRING,
  custrecord_nact_recon_approval_status BIGINT,
  custrecord_nact_recon_approved TIMESTAMP,
  custrecord_nact_recon_approved_by BIGINT,
  custrecord_nact_recon_approver STRING,
  custrecord_nact_recon_approvstatusnote STRING,
  custrecord_nact_recon_assignee BIGINT,
  custrecord_nact_recon_auto_rec_notes STRING,
  custrecord_nact_recon_bal_activity_expl STRING,
  custrecord_nact_recon_balance_diff_perc DOUBLE,
  custrecord_nact_recon_balance_difference DOUBLE,
  custrecord_nact_recon_completed_by BIGINT,
  custrecord_nact_recon_completenotes STRING,
  custrecord_nact_recon_completion_date TIMESTAMP,
  custrecord_nact_recon_credits DOUBLE,
  custrecord_nact_recon_currency_id BIGINT,
  custrecord_nact_recon_dayspastdue BIGINT,
  custrecord_nact_recon_daysuntildue BIGINT,
  custrecord_nact_recon_debits DOUBLE,
  custrecord_nact_recon_due TIMESTAMP,
  custrecord_nact_recon_explained DOUBLE,
  custrecord_nact_recon_gl_begin_balance DOUBLE,
  custrecord_nact_recon_gl_end_balance DOUBLE,
  custrecord_nact_recon_group_tag STRING,
  custrecord_nact_recon_is_restarted STRING,
  custrecord_nact_recon_item_create_status BIGINT,
  custrecord_nact_recon_message_json STRING,
  custrecord_nact_recon_method BIGINT,
  custrecord_nact_recon_normal_balance BIGINT,
  custrecord_nact_recon_parent_subsidiary BIGINT,
  custrecord_nact_recon_period_end_date TIMESTAMP,
  custrecord_nact_recon_period_id BIGINT,
  custrecord_nact_recon_period_start_date TIMESTAMP,
  custrecord_nact_recon_period_text STRING,
  custrecord_nact_recon_processing_status STRING,
  custrecord_nact_recon_risklevel BIGINT,
  custrecord_nact_recon_rule BIGINT,
  custrecord_nact_recon_segment_1_ids STRING,
  custrecord_nact_recon_segment_1_text STRING,
  custrecord_nact_recon_segment_1_type STRING,
  custrecord_nact_recon_status BIGINT,
  custrecord_nact_recon_subsidiary STRING,
  custrecord_nact_recon_task_id STRING,
  custrecord_nact_recon_unexplained DOUBLE,
  custrecord_nact_script_execution_log STRING,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_recon_bank_account BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_recon_clearing_info definition

CREATE TABLE bronze.ns.customrecord_nact_recon_clearing_info (
  abbreviation STRING,
  altname STRING,
  created TIMESTAMP,
  custrecord_nact_recon_clearing_amount DOUBLE,
  custrecord_nact_recon_clearing_credit STRING,
  custrecord_nact_recon_clearing_debit STRING,
  custrecord_nact_recon_clearing_tl_key BIGINT,
  custrecord_nact_recon_clearing_tran BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_recon_rules definition

CREATE TABLE bronze.ns.customrecord_nact_recon_rules (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_acct_group_abs_date BIGINT,
  custrecord_nact_acct_group_rel_date BIGINT,
  custrecord_nact_minimum_manual_review_mo BIGINT,
  custrecord_nact_rules_acc_bal_thresh DOUBLE,
  custrecord_nact_rules_account_field STRING,
  custrecord_nact_rules_account_owner BIGINT,
  custrecord_nact_rules_actvty_thresh DOUBLE,
  custrecord_nact_rules_applied_segment_1 STRING,
  custrecord_nact_rules_applied_subsidiary STRING,
  custrecord_nact_rules_approver STRING,
  custrecord_nact_rules_auto_notes STRING,
  custrecord_nact_rules_clear_acc_thresh DOUBLE,
  custrecord_nact_rules_frequency BIGINT,
  custrecord_nact_rules_inter_comp_thresh DOUBLE,
  custrecord_nact_rules_is_acc_bal_less STRING,
  custrecord_nact_rules_is_actvty_less_tha STRING,
  custrecord_nact_rules_is_bal_actvty_less STRING,
  custrecord_nact_rules_is_bank_matched STRING,
  custrecord_nact_rules_is_clear_acc_less STRING,
  custrecord_nact_rules_is_do_not_gen_ever STRING,
  custrecord_nact_rules_is_do_not_generate STRING,
  custrecord_nact_rules_is_equal_to_search STRING,
  custrecord_nact_rules_is_gl_unchanged STRING,
  custrecord_nact_rules_is_inter_comp_less STRING,
  custrecord_nact_rules_is_items_match STRING,
  custrecord_nact_rules_is_no_activity STRING,
  custrecord_nact_rules_is_not_auto_rec STRING,
  custrecord_nact_rules_is_seg_segment_1 STRING,
  custrecord_nact_rules_is_segment1_none STRING,
  custrecord_nact_rules_is_segr_account STRING,
  custrecord_nact_rules_is_segr_subsidiary STRING,
  custrecord_nact_rules_method BIGINT,
  custrecord_nact_rules_normal_balance BIGINT,
  custrecord_nact_rules_oim_item BIGINT,
  custrecord_nact_rules_parent BIGINT,
  custrecord_nact_rules_risk_level BIGINT,
  custrecord_nact_rules_search BIGINT,
  custrecord_nact_rules_search_column STRING,
  custrecord_nact_rules_search_date_field STRING,
  custrecord_nact_rules_segment_1 STRING,
  custrecord_nact_rules_segment_field STRING,
  custrecord_nact_rules_sub_consolidate BIGINT,
  custrecord_nact_rules_subsidiary_field STRING,
  custrecord_nact_rules_unexplained_thresh DOUBLE,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_rules_unex_perc_thresh DOUBLE,
  custrecord_nact_rules_folder_dim_2 STRING,
  custrecord_nact_rules_folder_dim_1 STRING,
  custrecord_nact_rules_accounting_book STRING,
  custrecord_nact_rules_search_book_field STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_reconitem definition

CREATE TABLE bronze.ns.customrecord_nact_reconitem (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_reconitem_amt DOUBLE,
  custrecord_nact_reconitem_bank_item BIGINT,
  custrecord_nact_reconitem_carryforward STRING,
  custrecord_nact_reconitem_created_from BIGINT,
  custrecord_nact_reconitem_desc STRING,
  custrecord_nact_reconitem_graph_file_id STRING,
  custrecord_nact_reconitem_graph_last_syn TIMESTAMP,
  custrecord_nact_reconitem_je BIGINT,
  custrecord_nact_reconitem_line_key BIGINT,
  custrecord_nact_reconitem_recon BIGINT,
  custrecord_nact_reconitem_search BIGINT,
  custrecord_nact_reconitem_transaction BIGINT,
  custrecord_nact_reconitem_type BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_relative_date definition

CREATE TABLE bronze.ns.customrecord_nact_relative_date (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_relativedate_netsuite_id STRING,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_report definition

CREATE TABLE bronze.ns.customrecord_nact_report (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_report_description STRING,
  custrecord_nact_report_external_id STRING,
  custrecord_nact_report_favorite STRING,
  custrecord_nact_report_help_link STRING,
  custrecord_nact_report_module BIGINT,
  custrecord_nact_report_preview_link STRING,
  custrecord_nact_report_route STRING,
  custrecord_nact_report_saved_search BIGINT,
  custrecord_nact_report_type BIGINT,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_report_wb_script_id STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.customrecord_nact_sent_email_automation definition

CREATE TABLE bronze.ns.customrecord_nact_sent_email_automation (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_sentemail_email_automate BIGINT,
  custrecord_nact_sentemail_task BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_sop definition

CREATE TABLE bronze.ns.customrecord_nact_sop (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_is_template STRING,
  custrecord_nact_sop_approval BIGINT,
  custrecord_nact_sop_definitions STRING,
  custrecord_nact_sop_implementationdate TIMESTAMP,
  custrecord_nact_sop_implementationperiod BIGINT,
  custrecord_nact_sop_lastreviewed TIMESTAMP,
  custrecord_nact_sop_owner STRING,
  custrecord_nact_sop_policy STRING,
  custrecord_nact_sop_prerequisites STRING,
  custrecord_nact_sop_procedure STRING,
  custrecord_nact_sop_purpose STRING,
  custrecord_nact_sop_references STRING,
  custrecord_nact_sop_responsibilities STRING,
  custrecord_nact_sop_revision STRING,
  custrecord_nact_sop_scope STRING,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_sop_step definition

CREATE TABLE bronze.ns.customrecord_nact_sop_step (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_completed STRING,
  custrecord_nact_completed_by BIGINT,
  custrecord_nact_completed_on TIMESTAMP,
  custrecord_nact_sop_step_created_from BIGINT,
  custrecord_nact_sop_step_desc STRING,
  custrecord_nact_sop_step_is_auto_gen STRING,
  custrecord_nact_sop_step_link STRING,
  custrecord_nact_sop_step_number DOUBLE,
  custrecord_nact_sop_step_owner STRING,
  custrecord_nact_sop_step_task BIGINT,
  custrecordnact_sop_step_sop BIGINT,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_sourcing definition

CREATE TABLE bronze.ns.customrecord_nact_sourcing (
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_sourcing_account_type STRING,
  custrecord_nact_sourcing_apply_impact BIGINT,
  custrecord_nact_sourcing_apply_to_sub STRING,
  custrecord_nact_sourcing_apply_to_type STRING,
  custrecord_nact_sourcing_apply_tran_type STRING,
  custrecord_nact_sourcing_from_field_id STRING,
  custrecord_nact_sourcing_label_to STRING,
  custrecord_nact_sourcing_to_field_id STRING,
  custrecord_nact_sourcing_type BIGINT,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_task definition

CREATE TABLE bronze.ns.customrecord_nact_task (
  custrecord_nact_task_completedate TIMESTAMP,
  custrecord_nact_task_department BIGINT,
  custrecord_nact_task_durationestimate DOUBLE,
  custrecord_nact_task_is_ignor_depen_task STRING,
  custrecord_nact_task_soplink STRING,
  _fivetran_deleted BOOLEAN,
  externalid STRING,
  lastmodifiedby BIGINT,
  custrecord_nact_task_frequency BIGINT,
  custrecord_nact_task_abs_date BIGINT,
  custrecord_nact_task_is_template STRING,
  custrecord_nact_task_start_date TIMESTAMP,
  scriptid STRING,
  custrecord_nact_task_duedatetime TIMESTAMP,
  custrecord_nact_task_risklevel BIGINT,
  id BIGINT,
  custrecord_nact_task_importance BIGINT,
  custrecord_nact_task_reviewnotes STRING,
  custrecord_nact_task_status BIGINT,
  custrecord_nact_task_subsidiary BIGINT,
  custrecord_nact_task_completedby BIGINT,
  created TIMESTAMP,
  custrecord_nact_task_activitylink STRING,
  custrecord_nact_task_dependency STRING,
  custrecord_nact_task_parent BIGINT,
  lastmodified TIMESTAMP,
  custrecord_nact_task_sort_key DOUBLE,
  name STRING,
  custrecord_nact_task_assignee BIGINT,
  custrecord_nact_task_template_reference BIGINT,
  custrecord_nact_task_due_date_lead_time BIGINT,
  custrecord_nact_task_is_ignor_assignee STRING,
  isinactive STRING,
  custrecord_nact_task_key STRING,
  custrecord_nact_task_reviewdate TIMESTAMP,
  custrecord_nact_task_description STRING,
  date_deleted TIMESTAMP,
  custrecord_nact_task_dependencynotes STRING,
  custrecord_nact_task_is_restarted STRING,
  custrecord_nact_task_durationactual DOUBLE,
  custrecord_nact_task_task_key STRING,
  custrecord_nact_tak_is_ignor_submit_revw STRING,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_task_class BIGINT,
  custrecord_nact_task_reviewer STRING,
  custrecord_nact_task_actual_end TIMESTAMP,
  custrecord_nact_task_close_date TIMESTAMP,
  custrecord_nact_task_acctgperiod BIGINT,
  custrecord_nact_task_actual_start TIMESTAMP,
  custrecord_nact_task_classification BIGINT,
  custrecord_nact_task_closetiming BIGINT,
  custrecord_nact_task_script_execution STRING,
  owner BIGINT,
  custrecord_nact_task_dependencystatus BIGINT,
  custrecord_nact_task_is_ignor_send_back STRING,
  custrecord_nact_task_location BIGINT,
  custrecord_nact_task_statuscomments STRING,
  abbreviation STRING,
  custrecord_nact_task_closegrouping BIGINT,
  custrecord_nact_task_type BIGINT,
  recordid BIGINT,
  custrecord_nact_task_sop BIGINT,
  custrecord_nact_procedure_template BIGINT,
  custrecord_nact_task_reference STRING,
  custrecord_nact_task_is_ignore_due_date STRING,
  custrecord_nact_task_weekday_due BIGINT,
  custrecord_nact_task_rev_due_rel BIGINT,
  custrecord_nact_task_submitted_due_date TIMESTAMP,
  custrecord_nact_task_holiday_preference BIGINT,
  custrecord_nact_task_reviewer_due_date TIMESTAMP,
  custrecord_nact_task_accounting_book BIGINT,
  custrecord_nact_task_rev_due_from_due BIGINT,
  custrecord_nact_task_rev_due_abs BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_task_activity_perm definition

CREATE TABLE bronze.ns.customrecord_nact_task_activity_perm (
  abbreviation STRING,
  created TIMESTAMP,
  externalid STRING,
  id BIGINT,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.customrecord_nact_taskclassification definition

CREATE TABLE bronze.ns.customrecord_nact_taskclassification (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_taskclassification_insrt BIGINT,
  custrecord_nact_taskclassification_order DOUBLE,
  custrecord_nact_taskclassification_prfx STRING,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecord_nact_work_calendar definition

CREATE TABLE bronze.ns.customrecord_nact_work_calendar (
  id BIGINT,
  abbreviation STRING,
  created TIMESTAMP,
  custrecord_nact_calendar_hours_per_day BIGINT,
  custrecord_nact_calendar_is_all_subs STRING,
  custrecord_nact_calendar_subsidiary STRING,
  custrecord_nact_calendar_work_days STRING,
  externalid STRING,
  isinactive STRING,
  lastmodified TIMESTAMP,
  lastmodifiedby BIGINT,
  name STRING,
  owner BIGINT,
  recordid BIGINT,
  scriptid STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_nact_calendar_start_time TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.customrecordtype definition

CREATE TABLE bronze.ns.customrecordtype (
  internalid BIGINT,
  allowattachments STRING,
  allowinlinedeleting STRING,
  allowinlineediting STRING,
  allowquicksearch STRING,
  description STRING,
  enablemailmerge STRING,
  includename STRING,
  isinactive STRING,
  isordered STRING,
  lastmodifieddate TIMESTAMP,
  name STRING,
  nopermissionrequired STRING,
  owner BIGINT,
  scriptid STRING,
  shownotes STRING,
  usepermissions STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  externalrolepermissionlevel BIGINT,
  publicpermissionlevel BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.department definition

CREATE TABLE bronze.ns.department (
  id BIGINT,
  externalid STRING,
  fullname STRING,
  includechildren STRING,
  isinactive STRING,
  lastmodifieddate TIMESTAMP,
  name STRING,
  parent BIGINT,
  subsidiary STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.entity definition

CREATE TABLE bronze.ns.entity (
  id BIGINT,
  altemail STRING,
  altname STRING,
  altphone STRING,
  comments STRING,
  contact BIGINT,
  customer BIGINT,
  datecreated TIMESTAMP,
  email STRING,
  employee BIGINT,
  entityid STRING,
  entitynumber BIGINT,
  entitytitle STRING,
  externalid STRING,
  fax STRING,
  firstname STRING,
  group BIGINT,
  homephone STRING,
  isinactive STRING,
  isperson STRING,
  lastmodifieddate TIMESTAMP,
  lastname STRING,
  middlename STRING,
  mobilephone STRING,
  othername BIGINT,
  parent BIGINT,
  phone STRING,
  salutation STRING,
  title STRING,
  toplevelparent BIGINT,
  type STRING,
  vendor BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  unsubscribe STRING,
  source STRING,
  globalsubscriptionstatus BIGINT,
  isunavailable STRING,
  fullname STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.entityaddress definition

CREATE TABLE bronze.ns.entityaddress (
  nkey BIGINT,
  addr1 STRING,
  addr2 STRING,
  addr3 STRING,
  addressee STRING,
  addrphone STRING,
  addrtext STRING,
  attention STRING,
  city STRING,
  country STRING,
  dropdownstate STRING,
  lastmodifieddate TIMESTAMP,
  override STRING,
  recordowner BIGINT,
  state STRING,
  zip STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.entitynote definition

CREATE TABLE bronze.ns.entitynote (
  entity BIGINT,
  id BIGINT,
  author BIGINT,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notedate TIMESTAMP,
  notetype BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  time TIMESTAMP)
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


-- bronze.ns.entitysubscriptions definition

CREATE TABLE bronze.ns.entitysubscriptions (
  _fivetran_id STRING,
  entity BIGINT,
  lastmodifieddate TIMESTAMP,
  subscribed STRING,
  subscription BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.item definition

CREATE TABLE bronze.ns.item (
  id BIGINT,
  amortizationperiod BIGINT,
  amortizationtemplate BIGINT,
  assetaccount BIGINT,
  atpmethod STRING,
  averagecost DOUBLE,
  billexchratevarianceacct BIGINT,
  billingschedule BIGINT,
  billpricevarianceacct BIGINT,
  billqtyvarianceacct BIGINT,
  class BIGINT,
  copydescription STRING,
  cost DOUBLE,
  costestimate DOUBLE,
  costestimatetype STRING,
  costingmethod STRING,
  costingmethoddisplay STRING,
  countryofmanufacture STRING,
  createddate TIMESTAMP,
  createexpenseplanson BIGINT,
  custreturnvarianceaccount BIGINT,
  deferralaccount BIGINT,
  demandmodifier DOUBLE,
  department BIGINT,
  description STRING,
  displayname STRING,
  dropshipexpenseaccount BIGINT,
  effectivebomcontrol STRING,
  enforceminqtyinternally STRING,
  expenseaccount BIGINT,
  expenseamortizationrule BIGINT,
  externalid STRING,
  fullname STRING,
  fxcost DOUBLE,
  gainlossaccount BIGINT,
  generateaccruals STRING,
  handlingcost DOUBLE,
  includechildren STRING,
  incomeaccount BIGINT,
  intercoexpenseaccount BIGINT,
  intercoincomeaccount BIGINT,
  isdropshipitem STRING,
  isfulfillable STRING,
  isinactive STRING,
  islotitem STRING,
  isonline STRING,
  isserialitem STRING,
  isspecialorderitem STRING,
  itemid STRING,
  itemtype STRING,
  lastmodifieddate TIMESTAMP,
  lastpurchaseprice DOUBLE,
  leadtime BIGINT,
  location BIGINT,
  manufacturer STRING,
  matchbilltoreceipt STRING,
  matrixitemnametemplate STRING,
  matrixtype STRING,
  maximumquantity BIGINT,
  minimumquantity BIGINT,
  mpn STRING,
  overallquantitypricingtype STRING,
  parent BIGINT,
  preferredlocation BIGINT,
  pricinggroup BIGINT,
  printitems STRING,
  prodpricevarianceacct BIGINT,
  prodqtyvarianceacct BIGINT,
  purchasedescription STRING,
  purchaseorderamount DOUBLE,
  purchaseorderquantity DOUBLE,
  purchaseorderquantitydiff DOUBLE,
  purchasepricevarianceacct BIGINT,
  purchaseunit BIGINT,
  quantitypricingschedule BIGINT,
  receiptamount DOUBLE,
  receiptquantity DOUBLE,
  receiptquantitydiff DOUBLE,
  residual DOUBLE,
  safetystocklevel DOUBLE,
  saleunit BIGINT,
  scrapacct BIGINT,
  seasonaldemand STRING,
  shipindividually STRING,
  shippackage BIGINT,
  shippingcost DOUBLE,
  stockdescription STRING,
  stockunit BIGINT,
  subsidiary STRING,
  subtype STRING,
  supplyreplenishmentmethod STRING,
  totalquantityonhand DOUBLE,
  totalvalue DOUBLE,
  tracklandedcost STRING,
  transferprice DOUBLE,
  unbuildvarianceaccount BIGINT,
  unitstype BIGINT,
  upccode STRING,
  usebins STRING,
  usecomponentyield STRING,
  usemarginalrates STRING,
  vendorname STRING,
  vendreturnvarianceaccount BIGINT,
  weight DOUBLE,
  weightunit BIGINT,
  weightunits STRING,
  wipacct BIGINT,
  wipvarianceacct BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  autoreorderpoint STRING,
  autoleadtime STRING,
  autopreferredstocklevel STRING,
  reordermultiple BIGINT,
  custitem_atlas_item_image BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.jobnote definition

CREATE TABLE bronze.ns.jobnote (
  entity BIGINT,
  id BIGINT,
  author BIGINT,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notedate TIMESTAMP,
  notetype BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  time TIMESTAMP)
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


-- bronze.ns.journaltransactions definition

CREATE TABLE bronze.ns.journaltransactions (
  _fivetran_id STRING,
  amount DOUBLE,
  createddate TIMESTAMP,
  details STRING,
  entity BIGINT,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  quantity DOUBLE,
  status STRING,
  statuscode STRING,
  subsidiary BIGINT,
  transactionnumber STRING,
  type STRING,
  typecode STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.location definition

CREATE TABLE bronze.ns.location (
  id BIGINT,
  defaultallocationpriority DOUBLE,
  externalid STRING,
  fullname STRING,
  isinactive STRING,
  lastmodifieddate TIMESTAMP,
  latitude DOUBLE,
  locationtype BIGINT,
  longitude DOUBLE,
  mainaddress BIGINT,
  makeinventoryavailable STRING,
  name STRING,
  parent BIGINT,
  returnaddress BIGINT,
  subsidiary STRING,
  tranprefix STRING,
  usebins STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.locationmainaddress definition

CREATE TABLE bronze.ns.locationmainaddress (
  nkey BIGINT,
  addr1 STRING,
  addr2 STRING,
  addr3 STRING,
  addressee STRING,
  addrphone STRING,
  addrtext STRING,
  attention STRING,
  city STRING,
  country STRING,
  dropdownstate STRING,
  lastmodifieddate TIMESTAMP,
  override STRING,
  recordowner BIGINT,
  state STRING,
  zip STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.mfgprojectnote definition

CREATE TABLE bronze.ns.mfgprojectnote (
  entity BIGINT,
  id BIGINT,
  author BIGINT,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notedate TIMESTAMP,
  notetype BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  time TIMESTAMP)
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


-- bronze.ns.nexttransactionaccountinglinelink definition

CREATE TABLE bronze.ns.nexttransactionaccountinglinelink (
  _fivetran_id STRING,
  amount DOUBLE,
  lastmodifieddate TIMESTAMP,
  linktype STRING,
  nextaccountingline STRING,
  nextdoc BIGINT,
  nextline BIGINT,
  nexttype STRING,
  previousaccountingline STRING,
  previousdoc BIGINT,
  previousline BIGINT,
  previoustype STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  accountingbook BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.previoustransactionaccountinglinelink definition

CREATE TABLE bronze.ns.previoustransactionaccountinglinelink (
  _fivetran_id STRING,
  amount DOUBLE,
  lastmodifieddate TIMESTAMP,
  linktype STRING,
  nextaccountingline STRING,
  nextdoc BIGINT,
  nextline BIGINT,
  nexttype STRING,
  previousaccountingline STRING,
  previousdoc BIGINT,
  previousline BIGINT,
  previoustype STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  accountingbook BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.recentactivity definition

CREATE TABLE bronze.ns.recentactivity (
  _fivetran_id STRING,
  amount DOUBLE,
  createddate TIMESTAMP,
  details STRING,
  entity BIGINT,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  quantity DOUBLE,
  status STRING,
  statuscode STRING,
  subdetails STRING,
  subsidiary BIGINT,
  transactionnumber STRING,
  type STRING,
  typecode STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.recenttransactions definition

CREATE TABLE bronze.ns.recenttransactions (
  _fivetran_id STRING,
  amount DOUBLE,
  createddate TIMESTAMP,
  details STRING,
  entity BIGINT,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  quantity DOUBLE,
  status STRING,
  statuscode STRING,
  subsidiary BIGINT,
  transactionnumber STRING,
  type STRING,
  typecode STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.recenttransactionswithoutjournal definition

CREATE TABLE bronze.ns.recenttransactionswithoutjournal (
  _fivetran_id STRING,
  amount DOUBLE,
  createddate TIMESTAMP,
  details STRING,
  entity BIGINT,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  quantity DOUBLE,
  status STRING,
  statuscode STRING,
  subsidiary BIGINT,
  transactionnumber STRING,
  type STRING,
  typecode STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.salesinvoiced definition

CREATE TABLE bronze.ns.salesinvoiced (
  uniquekey BIGINT,
  account BIGINT,
  amount DOUBLE,
  amountnet DOUBLE,
  class BIGINT,
  costestimate DOUBLE,
  department BIGINT,
  employee BIGINT,
  entity BIGINT,
  estgrossprofit DOUBLE,
  estgrossprofitpercent DOUBLE,
  item BIGINT,
  itemcount BIGINT,
  location BIGINT,
  memo STRING,
  postingperiod BIGINT,
  subsidiary BIGINT,
  trandate TIMESTAMP,
  tranline BIGINT,
  transaction BIGINT,
  type STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  ponumber STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.salesordered definition

CREATE TABLE bronze.ns.salesordered (
  uniquekey BIGINT,
  account BIGINT,
  amount DOUBLE,
  amountnet DOUBLE,
  class BIGINT,
  costestimate DOUBLE,
  department BIGINT,
  employee BIGINT,
  entity BIGINT,
  estgrossprofit DOUBLE,
  estgrossprofitpercent DOUBLE,
  item BIGINT,
  itemcount BIGINT,
  location BIGINT,
  memo STRING,
  subsidiary BIGINT,
  trandate TIMESTAMP,
  tranline BIGINT,
  transaction BIGINT,
  type STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  ponumber STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.salesterritory definition

CREATE TABLE bronze.ns.salesterritory (
  id BIGINT,
  descr STRING,
  inactive STRING,
  lastmodifieddate TIMESTAMP,
  matchall STRING,
  name STRING,
  priority BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.subsidiary definition

CREATE TABLE bronze.ns.subsidiary (
  id BIGINT,
  country STRING,
  currency BIGINT,
  custrecord_psg_lc_test_mode STRING,
  dropdownstate STRING,
  edition STRING,
  email STRING,
  externalid STRING,
  fax STRING,
  federalidnumber STRING,
  fiscalcalendar BIGINT,
  fullname STRING,
  glimpactlocking STRING,
  intercoaccount BIGINT,
  iselimination STRING,
  isinactive STRING,
  languagelocale STRING,
  lastmodifieddate TIMESTAMP,
  legalname STRING,
  mainaddress BIGINT,
  name STRING,
  parent BIGINT,
  purchaseorderamount DOUBLE,
  purchaseorderquantity DOUBLE,
  purchaseorderquantitydiff DOUBLE,
  receiptamount DOUBLE,
  receiptquantity DOUBLE,
  receiptquantitydiff DOUBLE,
  representingcustomer BIGINT,
  representingvendor BIGINT,
  returnaddress BIGINT,
  shippingaddress BIGINT,
  showsubsidiaryname STRING,
  ssnortin STRING,
  state STRING,
  state1taxnumber STRING,
  tranprefix STRING,
  url STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custrecord_subnav_subsidiary_logo BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.supportcasenote definition

CREATE TABLE bronze.ns.supportcasenote (
  activity BIGINT,
  id BIGINT,
  author BIGINT,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notedate TIMESTAMP,
  notetype BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  time TIMESTAMP)
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


-- bronze.ns.`transaction` definition

CREATE TABLE bronze.ns.transaction (
  id BIGINT,
  abbrevtype STRING,
  accountbasednumber STRING,
  actualshipdate TIMESTAMP,
  altsalestotal DOUBLE,
  approvalstatus BIGINT,
  balsegstatus BIGINT,
  billingaddress BIGINT,
  billingstatus STRING,
  bulkprocsubmission BIGINT,
  buyingreason BIGINT,
  buyingtimeframe BIGINT,
  closedate TIMESTAMP,
  createdby BIGINT,
  createddate TIMESTAMP,
  currency BIGINT,
  custbody_10184_customer_entity_bank BIGINT,
  custbody_11187_pref_entity_bank BIGINT,
  custbody_11724_bank_fee DOUBLE,
  custbody_11724_pay_bank_fees STRING,
  custbody_15529_emp_entity_bank BIGINT,
  custbody_15529_vendor_entity_bank BIGINT,
  custbody_15699_exclude_from_ep_process STRING,
  custbody_15889_cust_refund_entity_bank BIGINT,
  custbody_2663_reference_num STRING,
  custbody_9997_autocash_assertion_field STRING,
  custbody_9997_is_for_ep_dd STRING,
  custbody_9997_is_for_ep_eft STRING,
  custbody_9997_pfa_record BIGINT,
  custbody_date_lsa TIMESTAMP,
  custbody_esc_campaign_category BIGINT,
  custbody_gvo_hyperlinktoextwebsite STRING,
  custbody_gvo_mileage STRING,
  custbody_le_completion_date TIMESTAMP,
  custbody_le_deal_id BIGINT,
  custbody_le_lienholder BIGINT,
  custbody_le_state STRING,
  custbody_le_vsc_gap BIGINT,
  custbody_leaseend_closer BIGINT,
  custbody_leaseend_closer2 BIGINT,
  custbody_leaseend_closer2_commission DOUBLE,
  custbody_leaseend_closercommission DOUBLE,
  custbody_leaseend_financecommission DOUBLE,
  custbody_leaseend_financeperson BIGINT,
  custbody_leaseend_payoff_accnumber STRING,
  custbody_leaseend_setter BIGINT,
  custbody_leaseend_vinno STRING,
  custbody_leasened_settercommission DOUBLE,
  custbody_link_lsa STRING,
  custbody_link_name_lsa STRING,
  custbody_sas_journal_total DOUBLE,
  custbodyleaseend_stocknumber STRING,
  customform BIGINT,
  customtype BIGINT,
  daysopen BIGINT,
  daysoverduesearch BIGINT,
  duedate TIMESTAMP,
  email STRING,
  employee BIGINT,
  enddate TIMESTAMP,
  entity BIGINT,
  entitystatus BIGINT,
  estgrossprofit DOUBLE,
  estgrossprofitpercent DOUBLE,
  estimatedbudget DOUBLE,
  exchangerate DOUBLE,
  expectedclosedate TIMESTAMP,
  externalid STRING,
  fax STRING,
  firmed STRING,
  foreignamountpaid DOUBLE,
  foreignamountunpaid DOUBLE,
  foreigntotal DOUBLE,
  fulfillmenttype BIGINT,
  fxaltsalestotal DOUBLE,
  fxnetaltsalestotal DOUBLE,
  includeinforecast STRING,
  incoterm BIGINT,
  intercoadj STRING,
  intercostatus BIGINT,
  intercotransaction BIGINT,
  isbudgetapproved STRING,
  isfinchrg STRING,
  isreversal STRING,
  journaltype STRING,
  lastmodifiedby BIGINT,
  lastmodifieddate TIMESTAMP,
  leadsource BIGINT,
  linkedtrackingnumberlist STRING,
  memdoc BIGINT,
  memo STRING,
  message STRING,
  netaltsalestotal DOUBLE,
  nextapprover BIGINT,
  nextbilldate TIMESTAMP,
  nexus BIGINT,
  number BIGINT,
  ordpicked STRING,
  ordreceived STRING,
  otherrefnum STRING,
  paymenthold STRING,
  paymentlink STRING,
  paymentmethod BIGINT,
  paymentoption BIGINT,
  payrollbatch BIGINT,
  posting STRING,
  postingperiod BIGINT,
  printedpickingticket STRING,
  probability DOUBLE,
  recordtype STRING,
  reversal BIGINT,
  reversaldate TIMESTAMP,
  reversaldefer STRING,
  revision BIGINT,
  salesreadiness BIGINT,
  shipcarrier STRING,
  shipcomplete STRING,
  shipdate TIMESTAMP,
  shippingaddress BIGINT,
  source STRING,
  sourcetransaction BIGINT,
  startdate TIMESTAMP,
  status STRING,
  terms BIGINT,
  title STRING,
  tobeprinted STRING,
  tosubsidiary BIGINT,
  totalcostestimate DOUBLE,
  trackingnumberlist STRING,
  trandate TIMESTAMP,
  trandisplayname STRING,
  tranid STRING,
  transactionnumber STRING,
  transferlocation BIGINT,
  type STRING,
  typebaseddocumentnumber STRING,
  useitemcostastransfercost STRING,
  userevenuearrangement STRING,
  visibletocustomer STRING,
  void STRING,
  voided STRING,
  website BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custbody_nact_amortization BIGINT,
  custbody_nact_accrual BIGINT,
  custbody_nact_locking_reason STRING,
  custbody_nact_amortization_line BIGINT,
  custbody_nact_task BIGINT,
  custbodyramp_receipt_url STRING,
  completeddatetime TIMESTAMP,
  releaseddatetime TIMESTAMP,
  custbody_leaseend_source STRING,
  parentexpensealloc BIGINT,
  shipmethod BIGINT,
  fob STRING,
  amountunbilled DOUBLE,
  memorized STRING,
  needsbill STRING,
  prevdate TIMESTAMP,
  foreignamountpaidnopost DOUBLE,
  discountdate TIMESTAMP,
  applypickdecomposition STRING,
  custbody_le_csv_lines BIGINT,
  custbody_le_csv_processed BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.transactionaccountingline definition

CREATE TABLE bronze.ns.transactionaccountingline (
  transaction BIGINT,
  accountingbook BIGINT,
  transactionline BIGINT,
  account BIGINT,
  amount DOUBLE,
  amountlinked DOUBLE,
  amountpaid DOUBLE,
  amountunpaid DOUBLE,
  credit DOUBLE,
  debit DOUBLE,
  glauditnumber STRING,
  glauditnumberdate TIMESTAMP,
  glauditnumbersequence BIGINT,
  glauditnumbersetby BIGINT,
  lastmodifieddate TIMESTAMP,
  netamount DOUBLE,
  paymentamountunused DOUBLE,
  paymentamountused DOUBLE,
  posting STRING,
  processedbyrevcommit STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  accounttype STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.transactioncontact definition

CREATE TABLE bronze.ns.transactioncontact (
  contact BIGINT,
  contactrole BIGINT,
  email STRING,
  lastmodifieddate TIMESTAMP,
  transaction BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_id STRING,
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


-- bronze.ns.transactionline definition

CREATE TABLE bronze.ns.transactionline (
  transaction BIGINT,
  id BIGINT,
  accountinglinetype STRING,
  actualshipdate TIMESTAMP,
  amortizationenddate TIMESTAMP,
  amortizationresidual STRING,
  amortizationsched BIGINT,
  amortizstartdate TIMESTAMP,
  billeddate TIMESTAMP,
  billingschedule BIGINT,
  billvariancestatus STRING,
  blandedcost STRING,
  category BIGINT,
  class BIGINT,
  cleared STRING,
  cleareddate TIMESTAMP,
  closedate TIMESTAMP,
  commitinventory BIGINT,
  commitmentfirm STRING,
  costestimate DOUBLE,
  costestimaterate DOUBLE,
  costestimatetype STRING,
  createdfrom BIGINT,
  createdpo BIGINT,
  creditforeignamount DOUBLE,
  custcol_le_lienholder_vendor BIGINT,
  custcol_le_variance_report_memo BIGINT,
  debitforeignamount DOUBLE,
  department BIGINT,
  documentnumber STRING,
  donotdisplayline STRING,
  dropship STRING,
  eliminate STRING,
  entity BIGINT,
  estgrossprofit DOUBLE,
  estgrossprofitpercent DOUBLE,
  estimatedamount DOUBLE,
  expectedreceiptdate TIMESTAMP,
  expenseaccount BIGINT,
  foreignamount DOUBLE,
  foreignamountpaid DOUBLE,
  foreignamountunpaid DOUBLE,
  foreignpaymentamountunused DOUBLE,
  foreignpaymentamountused DOUBLE,
  fulfillable STRING,
  hasfulfillableitems STRING,
  inventoryreportinglocation BIGINT,
  isbillable STRING,
  isclosed STRING,
  iscogs STRING,
  iscustomglline STRING,
  isdisplayedinpaycheck STRING,
  isfullyshipped STRING,
  isfxvariance STRING,
  isinventoryaffecting STRING,
  isrevrectransaction STRING,
  item BIGINT,
  itemtype STRING,
  kitcomponent STRING,
  kitmemberof BIGINT,
  landedcostcategory BIGINT,
  landedcostperline STRING,
  linelastmodifieddate TIMESTAMP,
  linesequencenumber BIGINT,
  location BIGINT,
  mainline STRING,
  matchbilltoreceipt STRING,
  memo STRING,
  netamount DOUBLE,
  oldcommitmentfirm STRING,
  orderpriority DOUBLE,
  payitem BIGINT,
  paymentmethod BIGINT,
  price BIGINT,
  processedbyrevcommit STRING,
  quantity DOUBLE,
  quantitybackordered DOUBLE,
  quantitybilled DOUBLE,
  quantitycommitted DOUBLE,
  quantitypacked DOUBLE,
  quantitypicked DOUBLE,
  quantityrejected DOUBLE,
  quantityshiprecv DOUBLE,
  rate DOUBLE,
  rateamount DOUBLE,
  ratepercent DOUBLE,
  requestnote STRING,
  revenueelement BIGINT,
  specialorder STRING,
  subsidiary BIGINT,
  taxline STRING,
  transactiondiscount STRING,
  transactionlinetype STRING,
  transferorderitemlineid BIGINT,
  uniquekey BIGINT,
  units BIGINT,
  vsoeisestimate STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  custcol_nact_amortization BIGINT,
  custcol_nact_amortization_start_date TIMESTAMP,
  custcol_nact_amortization_type BIGINT,
  custcol_nact_amortization_end_date TIMESTAMP,
  custcol_nact_accrual BIGINT,
  custcol_nact_amortization_line BIGINT,
  custcol_nact_amort_hide_create_from_t STRING,
  custcol_nact_is_accrual_processed STRING,
  settlementamount DOUBLE,
  custcol_nact_amortization_cap_date TIMESTAMP,
  costestimatebase DOUBLE,
  duedate TIMESTAMP,
  isnonreimbursable STRING,
  excludefromraterequest STRING,
  periodclosed TIMESTAMP,
  donotprintline STRING,
  hascostline STRING,
  linecreateddate TIMESTAMP,
  foreignamountpaidnopost DOUBLE,
  foreignpaymentamountusednopost DOUBLE,
  landedcostsourcelineid BIGINT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');


-- bronze.ns.transactionnote definition

CREATE TABLE bronze.ns.transactionnote (
  transaction BIGINT,
  id BIGINT,
  author BIGINT,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notedate TIMESTAMP,
  notetype BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  time TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.usernotes_customlist72 definition

CREATE TABLE bronze.ns.usernotes_customlist72 (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist73 definition

CREATE TABLE bronze.ns.usernotes_customlist73 (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist74 definition

CREATE TABLE bronze.ns.usernotes_customlist74 (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist75 definition

CREATE TABLE bronze.ns.usernotes_customlist75 (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist76 definition

CREATE TABLE bronze.ns.usernotes_customlist76 (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist77 definition

CREATE TABLE bronze.ns.usernotes_customlist77 (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist1248 definition

CREATE TABLE bronze.ns.usernotes_customlist1248 (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist1249 definition

CREATE TABLE bronze.ns.usernotes_customlist1249 (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_acct_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_acct_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_approval_level definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_approval_level (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_approval_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_approval_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_bank_acct_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_bank_acct_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_bank_fee_code definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_bank_fee_code (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_batch_priority definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_batch_priority (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_batch_status definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_batch_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_billing_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_billing_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_day_of_week definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_day_of_week (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_direct_debit_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_direct_debit_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_entity_bank_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_entity_bank_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_ep_process definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_ep_process (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_output_file_encoding definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_output_file_encoding (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_payment_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_payment_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_process_status definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_process_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_recurrence definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_recurrence (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_reference_amended_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_reference_amended_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_schedule_event_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_schedule_event_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_schedule_type definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_schedule_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_2663_week_of_month definition

CREATE TABLE bronze.ns.usernotes_customlist_2663_week_of_month (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_8299_cat_type definition

CREATE TABLE bronze.ns.usernotes_customlist_8299_cat_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_8299_license_cache_status definition

CREATE TABLE bronze.ns.usernotes_customlist_8299_license_cache_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_11664_payment_mode definition

CREATE TABLE bronze.ns.usernotes_customlist_11664_payment_mode (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_12194_processing_result definition

CREATE TABLE bronze.ns.usernotes_customlist_12194_processing_result (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_15906_ep_log_type definition

CREATE TABLE bronze.ns.usernotes_customlist_15906_ep_log_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ach_ep_migration_recordtype definition

CREATE TABLE bronze.ns.usernotes_customlist_ach_ep_migration_recordtype (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_atlas_appr_by_creator definition

CREATE TABLE bronze.ns.usernotes_customlist_atlas_appr_by_creator (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_atlas_cust_type definition

CREATE TABLE bronze.ns.usernotes_customlist_atlas_cust_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_atlas_link definition

CREATE TABLE bronze.ns.usernotes_customlist_atlas_link (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_bf_api_types definition

CREATE TABLE bronze.ns.usernotes_customlist_bf_api_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_change_order_status definition

CREATE TABLE bronze.ns.usernotes_customlist_change_order_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_dt_tile_alertops definition

CREATE TABLE bronze.ns.usernotes_customlist_dt_tile_alertops (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_dt_tile_comparetype definition

CREATE TABLE bronze.ns.usernotes_customlist_dt_tile_comparetype (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_dt_tile_summarytype definition

CREATE TABLE bronze.ns.usernotes_customlist_dt_tile_summarytype (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_dt_tile_transflds definition

CREATE TABLE bronze.ns.usernotes_customlist_dt_tile_transflds (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_dt_tile_type definition

CREATE TABLE bronze.ns.usernotes_customlist_dt_tile_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_dt_translatable_flds definition

CREATE TABLE bronze.ns.usernotes_customlist_dt_translatable_flds (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_dt_wrapping definition

CREATE TABLE bronze.ns.usernotes_customlist_dt_wrapping (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ed_config_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ed_config_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ep_payment_priority definition

CREATE TABLE bronze.ns.usernotes_customlist_ep_payment_priority (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_esc_industries definition

CREATE TABLE bronze.ns.usernotes_customlist_esc_industries (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_esc_ratings definition

CREATE TABLE bronze.ns.usernotes_customlist_esc_ratings (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_le_vsc_gap definition

CREATE TABLE bronze.ns.usernotes_customlist_le_vsc_gap (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_accrual_create_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_accrual_create_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_accrual_status definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_accrual_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_amort_mod_status definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_amort_mod_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_amort_mod_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_amort_mod_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_amort_sl_transfer_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_amort_sl_transfer_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_amort_status definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_amort_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_amort_trans_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_amort_trans_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_approvstatus definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_approvstatus (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_avatar_colors definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_avatar_colors (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_bank_category_group definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_bank_category_group (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_bank_rule_flow_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_bank_rule_flow_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_bank_rule_match_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_bank_rule_match_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_bank_rule_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_bank_rule_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_closegrouping definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_closegrouping (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_days_of_week definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_days_of_week (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_expense_convention definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_expense_convention (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_flux_group_acct_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_flux_group_acct_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_flux_sc_acc_opt definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_flux_sc_acc_opt (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_flux_sc_acct_types definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_flux_sc_acct_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_flux_sc_groups definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_flux_sc_groups (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_flux_status definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_flux_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_importance definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_importance (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_normal_balance definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_normal_balance (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_oim_method definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_oim_method (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_period_preference definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_period_preference (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_recon_item_create_sts definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_recon_item_create_sts (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_recon_method definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_recon_method (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_reconciliations_freque definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_reconciliations_freque (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_reconitemtype definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_reconitemtype (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_reconstatus definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_reconstatus (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_report_module_list definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_report_module_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_report_type_list definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_report_type_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_risklevel definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_risklevel (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_sourcing_impacts definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_sourcing_impacts (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_sourcing_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_sourcing_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_status definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_task_email_trigger definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_task_email_trigger (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_task_frequency definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_task_frequency (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_task_holiday_pref definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_task_holiday_pref (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_tasktype definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_tasktype (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_upgrade_preferences definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_upgrade_preferences (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_nact_user_type definition

CREATE TABLE bronze.ns.usernotes_customlist_nact_user_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ibe_engagement_types definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ibe_engagement_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ibe_environment_types definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ibe_environment_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_one_change_log_rev_dec definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_one_change_log_rev_dec (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_one_change_log_type_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_one_change_log_type_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_one_req_qualification definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_one_req_qualification (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_issue_status_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_issue_status_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_issue_type_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_issue_type_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_milestone_schedule definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_milestone_schedule (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_milestone_status_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_milestone_status_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_one_stage_listing definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_one_stage_listing (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_priority_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_priority_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_projectype definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_projectype (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_risk_impact_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_risk_impact_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_risk_prob_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_risk_prob_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_risk_resolution_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_risk_resolution_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_risk_severity_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_risk_severity_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_status_code_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_status_code_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_pm_vertical definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_pm_vertical (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ps_action_item_priority definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ps_action_item_priority (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ps_action_item_status definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ps_action_item_status (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ps_cutover_status_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ps_cutover_status_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ps_process_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ps_process_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ps_uat_status_listing definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ps_uat_status_listing (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ts_base_status_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ts_base_status_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ts_bus_process_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ts_bus_process_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ts_deployed_to_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ts_deployed_to_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ts_feature_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ts_feature_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ts_fix_type_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ts_fix_type_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ts_issue_type_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ts_issue_type_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ts_priority_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ts_priority_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_ns_ts_test_phase_list definition

CREATE TABLE bronze.ns.usernotes_customlist_ns_ts_test_phase_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_sas_approval_action definition

CREATE TABLE bronze.ns.usernotes_customlist_sas_approval_action (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_sas_approvalmatrix_apptype definition

CREATE TABLE bronze.ns.usernotes_customlist_sas_approvalmatrix_apptype (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_sas_tol_reapproval_type definition

CREATE TABLE bronze.ns.usernotes_customlist_sas_tol_reapproval_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_sas_transaction_state definition

CREATE TABLE bronze.ns.usernotes_customlist_sas_transaction_state (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_sas_transaction_types definition

CREATE TABLE bronze.ns.usernotes_customlist_sas_transaction_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customlist_vb_memo_list definition

CREATE TABLE bronze.ns.usernotes_customlist_vb_memo_list (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_bank_details definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_bank_details (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_batch definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_batch (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_batch_details definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_batch_details (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_batch_transaction_info definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_batch_transaction_info (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_bill_eft_details definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_bill_eft_details (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_bill_eft_payment definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_bill_eft_payment (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_entity_bank_details definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_entity_bank_details (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_execution_log definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_execution_log (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_file_admin definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_file_admin (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_format_details definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_format_details (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_payment_aggregation definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_payment_aggregation (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_payment_file_format definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_payment_file_format (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_2663_payment_schedule definition

CREATE TABLE bronze.ns.usernotes_customrecord_2663_payment_schedule (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_8299_license_server definition

CREATE TABLE bronze.ns.usernotes_customrecord_8299_license_server (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_8391_dd_format_spcfc_types definition

CREATE TABLE bronze.ns.usernotes_customrecord_8391_dd_format_spcfc_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_10782_cntryfmts_by_edition definition

CREATE TABLE bronze.ns.usernotes_customrecord_10782_cntryfmts_by_edition (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_11724_bank_fee_sched definition

CREATE TABLE bronze.ns.usernotes_customrecord_11724_bank_fee_sched (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_12194_file_template_request definition

CREATE TABLE bronze.ns.usernotes_customrecord_12194_file_template_request (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_atlas_prl_import_job definition

CREATE TABLE bronze.ns.usernotes_customrecord_atlas_prl_import_job (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_bf_audittrail definition

CREATE TABLE bronze.ns.usernotes_customrecord_bf_audittrail (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_bf_se_customer_id definition

CREATE TABLE bronze.ns.usernotes_customrecord_bf_se_customer_id (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_bf_se_excluded_accounts definition

CREATE TABLE bronze.ns.usernotes_customrecord_bf_se_excluded_accounts (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_bf_se_next_transaction_id definition

CREATE TABLE bronze.ns.usernotes_customrecord_bf_se_next_transaction_id (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_bf_ydl_accept definition

CREATE TABLE bronze.ns.usernotes_customrecord_bf_ydl_accept (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_comp_fld_emp_payroll_tab definition

CREATE TABLE bronze.ns.usernotes_customrecord_comp_fld_emp_payroll_tab (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_dt_tile definition

CREATE TABLE bronze.ns.usernotes_customrecord_dt_tile (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_encryption_details definition

CREATE TABLE bronze.ns.usernotes_customrecord_encryption_details (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ep_company_information definition

CREATE TABLE bronze.ns.usernotes_customrecord_ep_company_information (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ep_preference definition

CREATE TABLE bronze.ns.usernotes_customrecord_ep_preference (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_le_mapping_bank_parent definition

CREATE TABLE bronze.ns.usernotes_customrecord_le_mapping_bank_parent (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_le_mapping_lienholder definition

CREATE TABLE bronze.ns.usernotes_customrecord_le_mapping_lienholder (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_le_variance_report_memos definition

CREATE TABLE bronze.ns.usernotes_customrecord_le_variance_report_memos (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_lsa definition

CREATE TABLE bronze.ns.usernotes_customrecord_lsa (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_accrual definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_accrual (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_accrual_automation definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_accrual_automation (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_accrual_modification definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_accrual_modification (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_accrualtype definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_accrualtype (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_amort definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_amort (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_amort_mod definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_amort_mod (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_amort_sl definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_amort_sl (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_amort_type definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_amort_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_bank definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_bank (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_bank_rule definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_bank_rule (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_bank_rule_match definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_bank_rule_match (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_bankacct definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_bankacct (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_bankmatch definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_bankmatch (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_bankmatchtran definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_bankmatchtran (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_banktran definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_banktran (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_close_control definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_close_control (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_dependent_task definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_dependent_task (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_email_automation definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_email_automation (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_flux_anlsys definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_flux_anlsys (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_flux_group definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_flux_group (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_flux_scenario definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_flux_scenario (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_global_settings definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_global_settings (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_non_working_days definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_non_working_days (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_recon definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_recon (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_recon_rules definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_recon_rules (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_reconitem definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_reconitem (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_relative_date definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_relative_date (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_report definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_report (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_sent_email_automation definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_sent_email_automation (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_sop definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_sop (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_sop_step definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_sop_step (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_sourcing definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_sourcing (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_task definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_task (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_taskclassification definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_taskclassification (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nact_work_calendar definition

CREATE TABLE bronze.ns.usernotes_customrecord_nact_work_calendar (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_accnt_config_types definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_accnt_config_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_account_config definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_account_config (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_act_conf_deply_type definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_act_conf_deply_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_bug_statuses definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_bug_statuses (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_gap_types definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_gap_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_linkage_types definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_linkage_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_project definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_project (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_project_statuses definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_project_statuses (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_req_categories definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_req_categories (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_req_channel_or_area definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_req_channel_or_area (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_req_linkage definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_req_linkage (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_req_plan_statuses definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_req_plan_statuses (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_req_statuses definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_req_statuses (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_req_types definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_req_types (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_requirement definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_requirement (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ibe_skill_type definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ibe_skill_type (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_imp_gap definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_imp_gap (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_imp_process definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_imp_process (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_imp_req_module definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_imp_req_module (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_pm_issues definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_pm_issues (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_pm_key_milestones definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_pm_key_milestones (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_pm_project_risks definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_pm_project_risks (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_pm_readinessscorecard definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_pm_readinessscorecard (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ps_cutov_detail definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ps_cutov_detail (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ps_meeting_notes definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ps_meeting_notes (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ps_mfg_action_items definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ps_mfg_action_items (
  id BIGINT,
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ps_mfg_cutover_items definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ps_mfg_cutover_items (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ps_mfg_uat_template definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ps_mfg_uat_template (
  id BIGINT,
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_req_bug_resolut_statuses definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_req_bug_resolut_statuses (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_req_bug_severities definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_req_bug_severities (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_ns_ts_test_issue definition

CREATE TABLE bronze.ns.usernotes_customrecord_ns_ts_test_issue (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nsapm_pts_setupsummary definition

CREATE TABLE bronze.ns.usernotes_customrecord_nsapm_pts_setupsummary (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nsapm_rpm_setup_date_range definition

CREATE TABLE bronze.ns.usernotes_customrecord_nsapm_rpm_setup_date_range (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nsapm_rpm_setup_general definition

CREATE TABLE bronze.ns.usernotes_customrecord_nsapm_rpm_setup_general (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nsapm_rpm_setup_recordpages definition

CREATE TABLE bronze.ns.usernotes_customrecord_nsapm_rpm_setup_recordpages (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nsapm_setup_employee_access definition

CREATE TABLE bronze.ns.usernotes_customrecord_nsapm_setup_employee_access (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nsapm_setup_parent definition

CREATE TABLE bronze.ns.usernotes_customrecord_nsapm_setup_parent (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_nsapm_setup_roles_access definition

CREATE TABLE bronze.ns.usernotes_customrecord_nsapm_setup_roles_access (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_paycode_box14_mapping definition

CREATE TABLE bronze.ns.usernotes_customrecord_paycode_box14_mapping (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_payroll_comp_fld_def definition

CREATE TABLE bronze.ns.usernotes_customrecord_payroll_comp_fld_def (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_payroll_comp_upd_rec definition

CREATE TABLE bronze.ns.usernotes_customrecord_payroll_comp_upd_rec (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_pd_default_link definition

CREATE TABLE bronze.ns.usernotes_customrecord_pd_default_link (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_pd_holiday definition

CREATE TABLE bronze.ns.usernotes_customrecord_pd_holiday (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_pd_link definition

CREATE TABLE bronze.ns.usernotes_customrecord_pd_link (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_pd_link_category definition

CREATE TABLE bronze.ns.usernotes_customrecord_pd_link_category (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_approval_history definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_approval_history (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_approval_preferences definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_approval_preferences (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_approvalmatrix definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_approvalmatrix (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_department_approver definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_department_approver (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_draft_approval definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_draft_approval (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_mapping_setup definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_mapping_setup (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_mirror_approval definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_mirror_approval (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_ss_exp_apprule definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_ss_exp_apprule (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_sas_ss_exp_mapping definition

CREATE TABLE bronze.ns.usernotes_customrecord_sas_ss_exp_mapping (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.usernotes_customrecord_wd_naw_workflow_excep definition

CREATE TABLE bronze.ns.usernotes_customrecord_wd_naw_workflow_excep (
  author BIGINT,
  date TIMESTAMP,
  direction BIGINT,
  externalid STRING,
  id BIGINT,
  lastmodifieddate TIMESTAMP,
  note STRING,
  notetype BIGINT,
  owner BIGINT,
  title STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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


-- bronze.ns.vendor definition

CREATE TABLE bronze.ns.vendor (
  id BIGINT,
  accountnumber STRING,
  altemail STRING,
  altname STRING,
  altphone STRING,
  balance DOUBLE,
  balanceprimary DOUBLE,
  category BIGINT,
  comments STRING,
  companyname STRING,
  contact BIGINT,
  contactlist STRING,
  creditlimit DOUBLE,
  currency BIGINT,
  custentity_11724_pay_bank_fees STRING,
  custentity_2663_eft_file_format BIGINT,
  custentity_2663_email_address_notif STRING,
  custentity_2663_payment_method STRING,
  custentity_9572_vendor_entitybank_format BIGINT,
  custentity_9572_vendor_entitybank_sub BIGINT,
  datecreated TIMESTAMP,
  defaultbankaccount BIGINT,
  defaultbillingaddress BIGINT,
  defaultshippingaddress BIGINT,
  defaultvendorpaymentaccount BIGINT,
  duplicate STRING,
  email STRING,
  emailpreference STRING,
  emailtransactions STRING,
  entityid STRING,
  entitynumber BIGINT,
  entitytitle STRING,
  expenseaccount BIGINT,
  externalid STRING,
  fax STRING,
  faxtransactions STRING,
  firstname STRING,
  globalsubscriptionstatus BIGINT,
  homephone STRING,
  incoterm BIGINT,
  intransitbalance DOUBLE,
  isautogeneratedrepresentingentity STRING,
  isinactive STRING,
  isjobresourcevend STRING,
  isperson STRING,
  lastmodifieddate TIMESTAMP,
  lastname STRING,
  legalname STRING,
  middlename STRING,
  mobilephone STRING,
  payablesaccount BIGINT,
  phone STRING,
  printoncheckas STRING,
  printtransactions STRING,
  purchaseorderamount DOUBLE,
  purchaseorderquantity DOUBLE,
  purchaseorderquantitydiff DOUBLE,
  receiptamount DOUBLE,
  receiptquantity DOUBLE,
  receiptquantitydiff DOUBLE,
  representingsubsidiary BIGINT,
  rolesforsearch STRING,
  salutation STRING,
  subsidiaryedition STRING,
  terms BIGINT,
  timeapprover BIGINT,
  title STRING,
  unbilledorders DOUBLE,
  unbilledordersprimary DOUBLE,
  url STRING,
  workcalendar BIGINT,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP,
  unsubscribe STRING)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.vendorcategory definition

CREATE TABLE bronze.ns.vendorcategory (
  id BIGINT,
  externalid STRING,
  isinactive STRING,
  istaxagency STRING,
  lastmodifieddate TIMESTAMP,
  name STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
  _fivetran_synced TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');


-- bronze.ns.vendorvendorpaymentinstrumentlist definition

CREATE TABLE bronze.ns.vendorvendorpaymentinstrumentlist (
  entityowner BIGINT,
  externalid STRING,
  id BIGINT,
  isdefault STRING,
  isinactive STRING,
  lastmodifieddate TIMESTAMP,
  mask STRING,
  memo STRING,
  _fivetran_deleted BOOLEAN,
  date_deleted TIMESTAMP,
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