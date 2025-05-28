# Databricks notebook source
# MAGIC %md
# MAGIC Notebook to combine Big Deal Notebooks

# COMMAND ----------

# MAGIC %run "/LeaseEnd DB/Big Deal - Lease End DB"

# COMMAND ----------

# MAGIC %run "/LeaseEnd DB/Big Deal - NS"

# COMMAND ----------

# MAGIC %md
# MAGIC Get the distinct rows by vin and get the duplicate rows by vin

# COMMAND ----------


from pyspark.sql.functions import lower, coalesce

le_df.createOrReplaceTempView("le_df")

distinct_df = spark.sql(f"""
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY lower(vin) ORDER BY (SELECT NULL)) AS row_num
    FROM le_df
) AS ranked_rows
WHERE row_num = 1;
""")

duplicates_df = spark.sql(f"""
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY lower(vin) ORDER BY (SELECT NULL)) AS row_num
    FROM le_df
) AS ranked_rows
WHERE row_num > 1;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Join nesuite data to distinct vin rows

# COMMAND ----------

from pyspark.sql.functions import lower, coalesce

joined_df = joined_df = distinct_df.join(ns_df, [(lower(distinct_df['vin']) == lower(ns_df['vins']))],"fullouter")
joined_df = joined_df.withColumn('vin', coalesce(joined_df["vins"], joined_df["vin"]))



# COMMAND ----------

# MAGIC %md
# MAGIC Populate id & completion_date for missing vins & drop extra columns

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr

# Update id where vin ILIKE '%MIssing%'
joined_df = joined_df.withColumn("id",
                   when(col("vin").like("%Missing%"), 0)
                   .otherwise(col("id")))
# Update completion_date_utc where vin ILIKE '%MIssing%'
joined_df = joined_df.withColumn("completion_date_utc",
                    when(col("vins").like("%Missing%"),
                    expr("MAKE_DATE(year, month, 1)")).otherwise(col("completion_date_utc")))

joined_df = joined_df.drop('month', 'year', 'vins')

# COMMAND ----------

# MAGIC %md
# MAGIC Add the duplicated rows back into the data
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, StringType, TimestampType, DateType, LongType

duplicates_df = duplicates_df \
.withColumn("4105_rev_reserve", lit(None).cast("double")) \
.withColumn("bank_buyout_fees_5520", lit(None).cast("double")) \
.withColumn("commission_5302", lit(None).cast("double")) \
.withColumn("cor_total", lit(None).cast("double")) \
.withColumn("customer_experience_5403", lit(None).cast("double")) \
.withColumn("direct_emp_benefits_5330", lit(None).cast("double")) \
.withColumn("direct_payroll_tax_5340", lit(None).cast("double")) \
.withColumn("doc_&_title_total_rev", lit(None).cast("double")) \
.withColumn("doc_fees_chargeback_rev_4130c", lit(None).cast("double")) \
.withColumn("doc_fees_rev_4130", lit(None).cast("double")) \
.withColumn("funding_clerks_5301", lit(None).cast("double")) \
.withColumn("gap_advance_5120a", lit(None).cast("double")) \
.withColumn("gap_advance_rev_4120a", lit(None).cast("double")) \
.withColumn("gap_chargeback_rev_4121", lit(None).cast("double")) \
.withColumn("gap_cor_5120", lit(None).cast("double")) \
.withColumn("gap_cost_rev_4120c", lit(None).cast("double")) \
.withColumn("gap_rev_4120", lit(None).cast("double")) \
.withColumn("gap_total", lit(None).cast("double")) \
.withColumn("gap_total_rev", lit(None).cast("double")) \
.withColumn("gap_volume_bonus_rev_4120b", lit(None).cast("double")) \
.withColumn("gross_margin", lit(None).cast("double")) \
.withColumn("gross_profit", lit(None).cast("double")) \
.withColumn("ic_payoff_team_5304", lit(None).cast("double")) \
.withColumn("ns_date", lit(None).cast("timestamp")) \
.withColumn("other_cor_total", lit(None).cast("double")) \
.withColumn("outbound_commission_5305", lit(None).cast("double")) \
.withColumn("payoff_variance_5400", lit(None).cast("double")) \
.withColumn("payoff_variance_total", lit(None).cast("double")) \
.withColumn("penalties_5404", lit(None).cast("double")) \
.withColumn("postage_5510", lit(None).cast("double")) \
.withColumn("r&a_total_cor", lit(None).cast("double")) \
.withColumn("r&a_total_rev", lit(None).cast("double")) \
.withColumn("registration_variance_5402", lit(None).cast("double")) \
.withColumn("reserve_bonus_rev_4106", lit(None).cast("double")) \
.withColumn("reserve_chargeback_rev_4107", lit(None).cast("double")) \
.withColumn("reserve_total_rev", lit(None).cast("double")) \
.withColumn("row_num", lit(None).cast("integer")) \
.withColumn("sales_guarantee_5303", lit(None).cast("double")) \
.withColumn("sales_tax_variance_5401", lit(None).cast("double")) \
.withColumn("title_clerks_5320", lit(None).cast("double")) \
.withColumn("titling_fees_rev_4141", lit(None).cast("double")) \
.withColumn("total_revenue", lit(None).cast("double")) \
.withColumn("vsc_advance_5110a", lit(None).cast("double")) \
.withColumn("vsc_advance_rev_4110a", lit(None).cast("double")) \
.withColumn("vsc_chargeback_rev_4111", lit(None).cast("double")) \
.withColumn("vsc_cor_5110", lit(None).cast("double")) \
.withColumn("vsc_cost_rev_4110c", lit(None).cast("double")) \
.withColumn("vsc_rev_4110", lit(None).cast("double")) \
.withColumn("vsc_total", lit(None).cast("double")) \
.withColumn("vsc_total_rev", lit(None).cast("double")) \
.withColumn("vsc_volume_bonus_rev_4110b", lit(None).cast("double")) \
.withColumn("direct_people_cost", lit(None).cast("double")) \
.withColumn("repo", lit(None).cast("double"))

# COMMAND ----------

duplicates_df = duplicates_df.select(joined_df.columns)

# COMMAND ----------

joined_df = joined_df.unionAll(duplicates_df)


# COMMAND ----------

# Dropping the silver_deal_table
# Need to just update the schema, but doing this for now. 
#spark.sql(f"DROP TABLE IF EXISTS {silver_deal_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC get the timestamp column types (to null out dates before 1902 and after 2038 (min and max dates for unix time))

# COMMAND ----------

'''
# Get the schema of the joined_df DataFrame
schema = joined_df.schema

# Initialize an empty list to store the date and timestamp columns
date_columns = []

# Iterate through each field in the DataFrame schema
for field in schema:
    # Check if the field data type is a date or timestamp type
    if field.dataType.typeName() == "date" or field.dataType.typeName() == "timestamp":
        # Add the column name to the list
        date_columns.append(field.name)
'''

# COMMAND ----------

# MAGIC %md
# MAGIC perform the nulling of bad dates

# COMMAND ----------

# Filter rows with date values outside the given range and null out the dates
from pyspark.sql.functions import when

joined_df = joined_df.withColumn('creation_date_utc', when((joined_df.creation_date_utc < '1902-01-01') | (joined_df.creation_date_utc > '2037-12-31'), None).otherwise(joined_df.creation_date_utc))
joined_df = joined_df.withColumn('completion_date_utc', when((joined_df.completion_date_utc < '1902-01-01') | (joined_df.completion_date_utc > '2037-12-31'), None).otherwise(joined_df.completion_date_utc))
joined_df = joined_df.withColumn('state_asof_utc', when((joined_df.state_asof_utc < '1902-01-01') | (joined_df.state_asof_utc > '2037-12-31'), None).otherwise(joined_df.state_asof_utc))
joined_df = joined_df.withColumn('dob', when((joined_df.dob < '1902-01-01') | (joined_df.dob > '2037-12-31'), None).otherwise(joined_df.dob))
joined_df = joined_df.withColumn('kbb_valuation_date', when((joined_df.kbb_valuation_date < '1902-01-01') | (joined_df.kbb_valuation_date > '2037-12-31'), None).otherwise(joined_df.kbb_valuation_date))
joined_df = joined_df.withColumn('registration_expiration', when((joined_df.registration_expiration < '1902-01-01') | (joined_df.registration_expiration > '2037-12-31'), None).otherwise(joined_df.registration_expiration))
joined_df = joined_df.withColumn('jdp_valuation_date', when((joined_df.jdp_valuation_date < '1902-01-01') | (joined_df.jdp_valuation_date > '2037-12-31'), None).otherwise(joined_df.jdp_valuation_date))
joined_df = joined_df.withColumn('first_payment_date', when((joined_df.first_payment_date < '1902-01-01') | (joined_df.first_payment_date > '2037-12-31'), None).otherwise(joined_df.first_payment_date))
joined_df = joined_df.withColumn('ns_invoice_date', when((joined_df.ns_invoice_date < '1902-01-01') | (joined_df.ns_invoice_date > '2037-12-31'), None).otherwise(joined_df.ns_invoice_date))
joined_df = joined_df.withColumn('approval_on_deal_processing', when((joined_df.approval_on_deal_processing < '1902-01-01') | (joined_df.approval_on_deal_processing > '2037-12-31'), None).otherwise(joined_df.approval_on_deal_processing))
joined_df = joined_df.withColumn('finished_documents_screen', when((joined_df.finished_documents_screen < '1902-01-01') | (joined_df.finished_documents_screen > '2037-12-31'), None).otherwise(joined_df.finished_documents_screen))
joined_df = joined_df.withColumn('reached_documents_screen', when((joined_df.reached_documents_screen < '1902-01-01') | (joined_df.reached_documents_screen > '2037-12-31'), None).otherwise(joined_df.reached_documents_screen))
joined_df = joined_df.withColumn('good_through_date', when((joined_df.good_through_date < '1902-01-01') | (joined_df.good_through_date > '2037-12-31'), None).otherwise(joined_df.good_through_date))
joined_df = joined_df.withColumn('next_payment_date', when((joined_df.next_payment_date < '1902-01-01') | (joined_df.next_payment_date > '2037-12-31'), None).otherwise(joined_df.next_payment_date))
joined_df = joined_df.withColumn('ns_date', when((joined_df.ns_date < '1902-01-01') | (joined_df.ns_date > '2037-12-31'), None).otherwise(joined_df.ns_date))

# COMMAND ----------

# If big_deal table exists, drop columns that are not in the big_deal table
if spark.catalog.tableExists(silver_deal_table):
    current = spark.read.table(silver_deal_table)
    cols_to_drop = []

    for col in joined_df.columns:
        if col not in current.columns:
            cols_to_drop.append(col)
            print(col)

    for c in cols_to_drop:
        joined_df = joined_df.drop(c)

# COMMAND ----------

joined_df.write.format("delta").mode("overwrite").saveAsTable(silver_deal_table)