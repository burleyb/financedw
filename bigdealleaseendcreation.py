# Databricks notebook source
deals_table = "bronze.leaseend_db_public.deals"
deal_states_table = "bronze.leaseend_db_public.deal_states"
car_table = "bronze.leaseend_db_public.cars"
# fi_table = "bronze.leasesend_db_public.financial_infos"
customers_table = "bronze.leaseend_db_public.customers"
addresses_table = "bronze.leaseend_db_public.addresses"
# customers_addresses_table = "bronze.leaseend_db_public.customers_addresses"
employments_table = "bronze.leaseend_db_public.employments"
# customers_employments_table = "bronze.leaseend_db_public.customers_employments"
financial_infos_table = "bronze.leaseend_db_public.financial_infos"
deals_referrals_sources_table = "bronze.leaseend_db_public.deals_referrals_sources"
payoffs_table = "bronze.leaseend_db_public.payoffs"

silver_deal_table = "silver.deal.big_deal"

# COMMAND ----------

def get_latest(df, key):
    cols_expr = [col for col in df.columns if col != key]
    select_expr_list = [f"{key}"] + [f"struct(updated_at, {', '.join(cols_expr)}) as other_cols"]
    df = df \
        .selectExpr(select_expr_list) \
        .groupBy(f"{key}") \
        .agg(max("other_cols").alias("latest")) \
        .select(f"{key}", "latest.*") \
        .drop('updated_at')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Basic Deal Info

# COMMAND ----------

# Start with basic deal info. 
deal_df = spark.read.table(deals_table).select("id","state","creation_date_utc","completion_date_utc","type","source").withColumnRenamed('state','deal_state')

basic_deal_count = deal_df.count()
print(basic_deal_count)

# COMMAND ----------

# MAGIC %md
# MAGIC Deal State

# COMMAND ----------

from pyspark.sql.functions import max, col
from pyspark.sql.types import TimestampType

# Get as-of time for the current deal state
ds_df = spark.read.table(deal_states_table)

ds_df = ds_df.withColumn('state_asof_utc', col('updated_date_utc').cast(TimestampType()))
ds_df = ds_df.groupBy('deal_id','state').agg(max('state_asof_utc').alias('state_asof_utc'))

deal_df = deal_df.join(ds_df,[deal_df.id == ds_df.deal_id, deal_df.deal_state == ds_df.state],"left").drop('deal_id','state')

# COMMAND ----------

# MAGIC %md
# MAGIC Basic Customer Info

# COMMAND ----------

# get customer info
customers_df = spark.sql(f"""
    SELECT 
        d.id as deal_id,
        c.id as customer_id,
        c.first_name,
        c.middle_name,
        c.last_name,
        c.name_suffix,
        c.dob,
        c.marital_status,
        c.phone_number,
        c.home_phone_number,
        c.email
    FROM {deals_table} as d
    LEFT JOIN {customers_table} as c on c.id = d.customer_id
""")

# COMMAND ----------

from pyspark.sql.functions import when, months_between, col, floor

customers_df = customers_df.withColumn("dob", when(customers_df.dob < '1900-01-01', None).otherwise(customers_df.dob))

# COMMAND ----------

deal_df = deal_df.join(customers_df,deal_df.id == customers_df.deal_id,"left").drop('deal_id')

# COMMAND ----------


from pyspark.sql.functions import when
# Calculate age of driver at start of deal.
deal_df = deal_df.withColumn("age", when((deal_df.dob.isNotNull()) & (deal_df.creation_date_utc.isNotNull()),floor(months_between("creation_date_utc", "dob") / 12)).otherwise(None))

# Probably need to take dates, but here is initial cut
deal_df = deal_df.withColumn("age", when(deal_df.age <= 0,None).otherwise(deal_df.age))

# COMMAND ----------

# MAGIC %md
# MAGIC finscore
# MAGIC

# COMMAND ----------


fin_score_df = spark.sql(f"""
    WITH ORDERED_CONTACTS AS (
        select contact_id,
        finscore,
        row_number() over (partition by contact_id ORDER BY CASE WHEN finscore IS NOT NULL THEN source_date END DESC) as row_num
        from silver.contact_db.contact_snapshot
        
    ), MAPPER AS (
        select from_id as customer_id, to_id as contact_id
        from silver.mapper.global_mapper
        where from_type = 'CUSTOMER' 
        and to_type = 'CONTACT'
    )
    select MAPPER.customer_id as cust_id, ORDERED_CONTACTS.finscore 
    from ORDERED_CONTACTS
    inner join MAPPER on ORDERED_CONTACTS.contact_id = MAPPER.contact_id
    where ORDERED_CONTACTS.row_num = 1
    
""")

deal_df = deal_df.join(fin_score_df, deal_df.customer_id == fin_score_df.cust_id, "left").drop("cust_id")


# COMMAND ----------

# MAGIC %md
# MAGIC Address Info

# COMMAND ----------

from pyspark.sql.functions import max

cust_df = spark.sql(f"""
    SELECT 
        a.customer_id as customer_id2,
        a.address_line,
        a.address_line_2,
        a.city,
        a.state,
        a.zip,
        a.county,
        a.residence_type,
        a.years_at_home,
        a.months_at_home,
        a.monthly_payment,
        a.time_zone,
        a.updated_at
    FROM {addresses_table} as a
    WHERE a.address_type = 'current'
""")

# The below logic is used to make sure that we take the latest 'current' address.
# Need this becuase we sometimes have multiple current addresses for customers. 
cust_df = get_latest(cust_df,'customer_id2')

deal_df = deal_df.join(cust_df,deal_df.customer_id == cust_df.customer_id2,"left").drop('customer_id2')

# COMMAND ----------

# MAGIC %md
# MAGIC Employment Info

# COMMAND ----------

# get customer employment info
cust_df = spark.sql(f"""
    SELECT 
        e.customer_id as customer_id2,
        e.name as employer,
        e.job_title,
        e.phone_number as work_phone_number,
        e.years_at_job,
        e.months_at_job,
        e.gross_income,
        e.pay_frequency,
        e.status as employment_status,
        e.updated_at
    FROM {employments_table} as e
    WHERE e.employment_type = 'current'
""")

cust_df = get_latest(cust_df,'customer_id2')

deal_df = deal_df.join(cust_df,deal_df.customer_id == cust_df.customer_id2,"left").drop('customer_id2')

# COMMAND ----------

# MAGIC %md
# MAGIC Car Info

# COMMAND ----------

from pyspark.sql.functions import upper
# Join car info
car_df = spark.read.table(car_table).drop("hubspot_id","_fivetran_deleted","_fivetran_synced","created_at","updated_at","id").drop('kbb_selected_options','kbb_selected_options_object')

car_df = car_df.withColumn("make", upper(car_df["make"]))
car_df = car_df.withColumn("model", upper(car_df["model"]))
car_df = car_df.withColumn("model_year", car_df["year"].cast("integer")).drop('year')

# COMMAND ----------

deal_df = deal_df.join(car_df, deal_df.id == car_df.deal_id, "left").drop("deal_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Financial Infos

# COMMAND ----------

# Join financial_infos
fi_df = spark.read.table(financial_infos_table) \
    .drop('id','deprecated_taxes','deprecated_taxes_no_warranty','pen_vsc_session_id','pen_vsc_rate_id','pen_vsc_form_id','pen_gap_session_id','pen_gap_rate_id','pen_gap_form_id','pen_vsc_contract_number','pen_gap_contract_number','created_at','tt_transaction_id','selected_credit_decision_id','_fivetran_deleted','_fivetran_synced')

# COMMAND ----------

from pyspark.sql.functions import when

fi_df = fi_df.withColumn("ally_fees",
    when(fi_df.option_type == 'vsc', 675)
    .when(fi_df.option_type == 'gap', 50)
    .when(fi_df.option_type == 'vscPlusGap', 725)
    .otherwise(0))

fi_df = fi_df.withColumn("rpt", when(fi_df.title.isNull(), 0).otherwise(fi_df.title) + \
    when(fi_df.doc_fee.isNull(), 0).otherwise(fi_df.doc_fee) + \
    when(fi_df.profit.isNull(), 0).otherwise(fi_df.profit) + \
    when(fi_df.ally_fees.isNull(), 0).otherwise(fi_df.ally_fees))

fi_df = fi_df.withColumn("vsc_rev",
    when((fi_df.option_type == 'vsc') | (fi_df.option_type == 'vscPlusGap'), fi_df.vsc_price - fi_df.vsc_cost)
    .otherwise(0))

fi_df = fi_df.withColumn("gap_rev",
    when((fi_df.option_type == 'gap') | (fi_df.option_type == 'vscPlusGap'), fi_df.gap_price - fi_df.gap_cost)
    .otherwise(0))


# COMMAND ----------

fi_df = get_latest(fi_df,'deal_id')

# COMMAND ----------

deal_df = deal_df.join(fi_df, deal_df.id == fi_df.deal_id, "left").drop("deal_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Get Deal Referral Sources

# COMMAND ----------

ref_df = spark.read.table(deals_referrals_sources_table) \
    .drop('id','created_at','_fivetran_deleted','_fivetran_synced')

# COMMAND ----------

ref_df = get_latest(ref_df,'deal_id')

# COMMAND ----------

deal_df = deal_df.join(ref_df, deal_df.id == ref_df.deal_id, "left").drop("deal_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Get Payoff Info

# COMMAND ----------

payoffs_df = spark.read.table(payoffs_table) \
    .select('lienholder_name','vehicle_payoff','good_through_date','lease_term','remaining_payments','msrp','residual_percentage','sales_price','residual_amount','estimated_payoff','vehicle_cost','old_lease_payment','next_payment_date','user_entered_total_payoff','lienholder_slug','car_id','updated_at')

# COMMAND ----------

payoffs_df = payoffs_df.withColumn("good_through_date", when(payoffs_df.good_through_date < '1900-01-01', None).otherwise(payoffs_df.good_through_date))

# COMMAND ----------

car_df = spark.read.table(car_table).select(col('id').alias('car_id'),'deal_id')

# COMMAND ----------

payoffs_df = payoffs_df.join(car_df,payoffs_df.car_id == car_df.car_id,"left").drop('car_id')

# COMMAND ----------

payoffs_df = get_latest(payoffs_df,'deal_id')

# COMMAND ----------

deal_df = deal_df.join(payoffs_df,deal_df.id == payoffs_df.deal_id,"left").drop('deal_id')

# COMMAND ----------

final_deal_count = deal_df.count()
if basic_deal_count != final_deal_count:
    print("Error on deal count")
else:
    print("Deal count good")

# COMMAND ----------

# Dropping the silver_deal_table
# Need to just update the schema, but doing this for now.  
# spark.sql(f"DROP TABLE IF EXISTS {silver_deal_table}")

# COMMAND ----------

#deal_df.write.format("delta").mode("overwrite").saveAsTable(silver_deal_table)

# COMMAND ----------

le_df = deal_df

# COMMAND ----------

# MAGIC %md
# MAGIC Still to add.
# MAGIC * credit app / decision.
# MAGIC     * These would be joined on credit_applications.r1_conversation_id and credit_decisions.r1_conversation_id

# COMMAND ----------

