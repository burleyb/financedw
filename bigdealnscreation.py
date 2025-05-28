# Databricks notebook source
# MAGIC %md
# MAGIC Vin Accounts

# COMMAND ----------

#these are the accounts we can get to the vin level non missing vin accounts

PerVinSelectedAccounts_df = spark.sql(f"""WITH GetVins AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        MAX(t.trandate) AS ns_date
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
Reserves AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 236
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
DocFee AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 239
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
TitlingFee AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 451
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
RevVSC AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 237
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
VSCBonus AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 479
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
GAPrev AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 238
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
GAPbonus AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 480
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
PayoffVariance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 532
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
SalesTaxVariance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 533
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
RegistrationVariance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 534
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
CustomerExperience AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 538
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
Penalties AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 539
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
TitlingFeeCor AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 452
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
BankBuyoutFees AS (
    SELECT 
        t.custbody_leaseend_vinno as vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 447
        and t.abbrevtype != 'PURCHORD'
        and t.abbrevtype != 'SALESORD'
        and t.abbrevtype != 'RTN AUTH'
        and (t.approvalstatus = 2 or t.approvalstatus is null)
        and length(t.custbody_leaseend_vinno) = 17
    group by
        t.custbody_leaseend_vinno
),
CORvsc AS (
    SELECT 
        t.custbody_leaseend_vinno as vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 267
        AND t.abbrevtype != 'PURCHORD'
        AND (t.approvalstatus = 2 or t.approvalstatus is null)
        AND length(t.custbody_leaseend_vinno) = 17
    group by
        t.custbody_leaseend_vinno
),
CORVSCadvance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 498
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
CORgap AS (
    SELECT 
        t.custbody_leaseend_vinno as vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 269
        AND t.abbrevtype != 'PURCHORD'
        AND t.abbrevtype != 'SALESORD'
        AND t.abbrevtype != 'RTN AUTH'
        AND t.abbrevtype != 'VENDAUTH'
        AND (t.approvalstatus = 2 or t.approvalstatus is null)
        AND length(t.custbody_leaseend_vinno) = 17
    group by
        t.custbody_leaseend_vinno
),
CORadvance AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 499
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
),
Repo AS (
    SELECT
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END AS vins,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 551
    GROUP BY
        CASE
            WHEN LENGTH(t.custbody_leaseend_vinno) = 17 THEN t.custbody_leaseend_vinno
            ELSE NULL
        END
)
SELECT
    GetVins.ns_date,
    MONTH(GetVins.ns_date) AS month,
    YEAR(GetVins.ns_date) AS year,
    GetVins.vins,
    COALESCE(r.total_amount, 0) as `4105_rev_reserve`,
    COALESCE(df.total_amount, 0) as `doc_fees_rev_4130`,
    COALESCE(tf.total_amount, 0) as `titling_fees_rev_4141`,
    COALESCE(rv.total_amount, 0) as `vsc_rev_4110`,
    COALESCE(vb.total_amount, 0) as `vsc_volume_bonus_rev_4110b`,
    COALESCE(gr.total_amount, 0) as `gap_rev_4120`,
    COALESCE(gb.total_amount, 0) as `gap_volume_bonus_rev_4120b`,
    COALESCE(pov.total_amount, 0) as `payoff_variance_5400`,
    COALESCE(stv.total_amount, 0) as `sales_tax_variance_5401`,
    COALESCE(rvar.total_amount, 0) as `registration_variance_5402`,
    COALESCE(ce.total_amount, 0) as `customer_experience_5403`,
    COALESCE(p.total_amount, 0) as `penalties_5404`,
    COALESCE(tfc.total_amount, 0) as `titling_fees_5141`,
    COALESCE(bbf.total_amount, 0) as `bank_buyout_fees_5520`,
    COALESCE(cvsc.total_amount, 0) as `vsc_cor_5110`,
    COALESCE(cvsca.total_amount, 0) as `vsc_advance_5110a`,
    COALESCE(cg.total_amount, 0) as `gap_cor_5120`,
    COALESCE(ca.total_amount, 0) as `gap_advance_5120a`,
    COALESCE(repo.total_amount, 0) as `repo`
FROM
    GetVins
    LEFT JOIN Reserves AS r ON GetVins.vins = r.vins
    LEFT JOIN DocFee AS df ON GetVins.vins = df.vins
    LEFT JOIN TitlingFee tf ON GetVins.vins = tf.vins
    LEFT JOIN RevVSC rv ON GetVins.vins = rv.vins
    LEFT JOIN VSCBonus vb ON GetVins.vins = vb.vins
    LEFT JOIN GAPrev gr ON GetVins.vins = gr.vins
    LEFT JOIN GAPbonus gb ON GetVins.vins = gb.vins
    LEFT JOIN PayoffVariance pov ON GetVins.vins = pov.vins
    LEFT JOIN SalesTaxVariance stv ON GetVins.vins = stv.vins
    LEFT JOIN RegistrationVariance rvar ON GetVins.vins = rvar.vins
    LEFT JOIN CustomerExperience ce ON GetVins.vins = ce.vins
    LEFT JOIN Penalties p ON GetVins.vins = p.vins
    LEFT JOIN TitlingFeeCor tfc ON GetVins.vins = tfc.vins
    LEFT JOIN BankBuyoutFees bbf ON GetVins.vins = bbf.vins
    LEFT JOIN CORvsc cvsc ON GetVins.vins = cvsc.vins
    LEFT JOIN CORVSCadvance cvsca ON GetVins.vins = cvsca.vins
    LEFT JOIN CORgap cg ON GetVins.vins = cg.vins
    LEFT JOIN CORadvance ca ON GetVins.vins = ca.vins
    LEFT JOIN Repo repo ON Repo.vins = ca.vins    
WHERE
    GetVins.vins IS NOT NULL
ORDER BY
    GetVins.ns_date;""")

#PerVinSelectedAccounts_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Get the completion dates and vins from DD.  Update per vin account date to use the completion date, not the transaction date
# MAGIC
# MAGIC

# COMMAND ----------



completion_date_df = spark.sql(f"""select d.completion_date_utc, cars.vin from bronze.leaseend_db_public.cars 
left join bronze.leaseend_db_public.deals d on cars.deal_id = d.id
where d.completion_date_utc is not null""")

# COMMAND ----------

from pyspark.sql.functions import coalesce, col

PerVinSelectedAccounts_df = PerVinSelectedAccounts_df.join(completion_date_df, PerVinSelectedAccounts_df.vins == completion_date_df.vin, "left")

PerVinSelectedAccounts_df = PerVinSelectedAccounts_df.dropDuplicates(["vins"])

PerVinSelectedAccounts_df = PerVinSelectedAccounts_df.withColumn("ns_date", coalesce(col("completion_date_utc"), col("ns_date")))

PerVinSelectedAccounts_df = PerVinSelectedAccounts_df.drop("completion_date_utc", "vin")


# COMMAND ----------

# MAGIC %md
# MAGIC Average Accounts

# COMMAND ----------

#this is the accounts that are average
AverageAccounts_df = spark.sql(f"""WITH GetVins AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        COUNT(t.custbody_leaseend_vinno) AS drivers
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.amount >= 699.00
        AND so.account = 239
        AND LENGTH(t.custbody_leaseend_vinno) = 17
        AND t.custbody_leaseend_vinno NOT IN (
            SELECT
                custbody_leaseend_vinno
            FROM
                bronze.ns.salesinvoiced AS so
                INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
            WHERE
                so.type = 'CustCred'
                AND MONTH(so.trandate) = MONTH(CURRENT_DATE)
                AND YEAR(so.trandate) = YEAR(CURRENT_DATE)
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
    UNION
    ALL
    SELECT
        1 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        2 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        3 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        4 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        5 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        6 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        7 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        8 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        9 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        10 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        11 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        12 as month,
        2021 as year,
        0 as drivers
    UNION
    ALL
    SELECT
        1 as month,
        2022 as year,
        0 as drivers
),
RevBonus AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 474
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
RevChargeback AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 524
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
DocFeeChargeback AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 517
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
VSCadvance AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 546
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
VSCcost AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 545
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
VSCchargeback AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 544
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
GAPadvance AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 547
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
GAPcost AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 548
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
GAPchargeback AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 549
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
RebatesAndDiscounts AS (
    SELECT
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(so.amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        so.account = 563
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
FundingClerks AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 528
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
Commission AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 552
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
SalesPersonGuarantee AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 553
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
ICPayoffTeam AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 556
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
OutboundCommision AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 564
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
TitleClerks AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 529
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
DirectEMPBenefits AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 530
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
DirectPayrollTax AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 531
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
Postage AS (
    SELECT
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 541
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
)
SELECT
    COALESCE(GetVins.month, rb.month, rcb.month) as avg_month,
    COALESCE(GetVins.year, rb.year, rcb.year) as avg_year,
    COALESCE(GetVins.drivers, 0) AS drivers,
    COALESCE(rb.total_amount, 0) AS `4106`,
    COALESCE(
        rb.total_amount / nullif(GetVins.drivers, 0),
        rb.total_amount,
        0
    ) as `reserve_bonus_rev_4106`,
    COALESCE(rcb.total_amount, 0) as `4107`,
    COALESCE(
        rcb.total_amount / nullif(GetVins.drivers, 0),
        rcb.total_amount,
        0
    ) as `reserve_chargeback_rev_4107`,
    COALESCE(dfcb.total_amount, 0) as `4130c`,
    COALESCE(
        dfcb.total_amount / nullif(GetVins.drivers, 0),
        dfcb.total_amount,
        0
    ) as `doc_fees_chargeback_rev_4130c`,
    COALESCE(vsca.total_amount, 0) as `4110a`,
    COALESCE(
        vsca.total_amount / nullif(GetVins.drivers, 0),
        vsca.total_amount,
        0
    ) as `vsc_advance_rev_4110a`,
    COALESCE(vscc.total_amount, 0) as `4110c`,
    COALESCE(
        vscc.total_amount / nullif(GetVins.drivers, 0),
        vscc.total_amount,
        0
    ) as `vsc_cost_rev_4110c`,
    COALESCE(vsccb.total_amount, 0) as `4111`,
    COALESCE(
        vsccb.total_amount / nullif(GetVins.drivers, 0),
        vsccb.total_amount,
        0
    ) as `vsc_chargeback_rev_4111`,
    COALESCE(ga.total_amount, 0) as `4120a`,
    COALESCE(
        ga.total_amount / nullif(GetVins.drivers, 0),
        ga.total_amount,
        0
    ) as `gap_advance_rev_4120a`,
    COALESCE(gc.total_amount, 0) as `4120c`,
    COALESCE(
        gc.total_amount / nullif(GetVins.drivers, 0),
        gc.total_amount,
        0
    ) as `gap_cost_rev_4120c`,
    COALESCE(gcb.total_amount, 0) as `4121`,
    COALESCE(
        gcb.total_amount / nullif(GetVins.drivers, 0),
        gcb.total_amount,
        0
    ) as `gap_chargeback_rev_4121`,
    COALESCE(rad.total_amount, 0) as `4190`,
    COALESCE(
        rad.total_amount / nullif(GetVins.drivers, 0),
        rad.total_amount,
        0
    ) as `4190_average`,
    COALESCE(fc.total_amount, 0) as `5301`,
    COALESCE(
        fc.total_amount / nullif(GetVins.drivers, 0),
        fc.total_amount,
        0
    ) as `funding_clerks_5301`,
    COALESCE(com.total_amount, 0) as `5302`,
    COALESCE(
        com.total_amount / nullif(GetVins.drivers, 0),
        com.total_amount,
        0
    ) as `commission_5302`,
    COALESCE(spg.total_amount, 0) as `5303`,
    COALESCE(
        spg.total_amount / nullif(GetVins.drivers, 0),
        spg.total_amount,
        0
    ) as `sales_guarantee_5303`,
    COALESCE(icpt.total_amount, 0) as `5304`,
    COALESCE(
        icpt.total_amount / nullif(GetVins.drivers, 0),
        icpt.total_amount,
        0
    ) as `ic_payoff_team_5304`,
    COALESCE(oc.total_amount, 0) as `5305`,
    COALESCE(
        oc.total_amount / nullif(GetVins.drivers, 0),
        oc.total_amount,
        0
    ) as `outbound_commission_5305`,
    COALESCE(tc.total_amount, 0) as `5320`,
    COALESCE(
        tc.total_amount / nullif(GetVins.drivers, 0),
        tc.total_amount,
        0
    ) as `title_clerks_5320`,
    COALESCE(dempb.total_amount, 0) as `5330`,
    COALESCE(
        dempb.total_amount / nullif(GetVins.drivers, 0),
        dempb.total_amount,
        0
    ) as `direct_emp_benefits_5330`,
    COALESCE(dpt.total_amount, 0) as `5340`,
    COALESCE(
        dpt.total_amount / nullif(GetVins.drivers, 0),
        dpt.total_amount,
        0
    ) as `direct_payroll_tax_5340`, 
    COALESCE(postage.total_amount, 0) as `5510`,
    COALESCE(
        postage.total_amount / nullif(GetVins.drivers, 0),
        postage.total_amount,
        0
    ) as `postage_5510`
FROM
    GetVins
    LEFT JOIN RevBonus AS rb ON GetVins.month = rb.month
    AND GetVins.year = rb.year
    LEFT JOIN RevChargeback AS rcb ON GetVins.month = rcb.month
    AND GetVins.year = rcb.year
    LEFT JOIN DocFeeChargeback AS dfcb ON GetVins.month = dfcb.month
    AND GetVins.year = dfcb.year
    LEFT JOIN VSCadvance AS vsca ON GetVins.month = vsca.month
    AND GetVins.year = vsca.year
    LEFT JOIN VSCcost AS vscc ON GetVins.month = vscc.month
    AND GetVins.year = vscc.year
    LEFT JOIN VSCchargeback AS vsccb ON GetVins.month = vsccb.month
    AND GetVins.year = vsccb.year
    LEFT JOIN GAPadvance AS ga ON GetVins.month = ga.month
    AND GetVins.year = ga.year
    LEFT JOIN GAPcost AS gc ON GetVins.month = gc.month
    AND GetVins.year = gc.year
    LEFT JOIN GAPchargeback AS gcb ON GetVins.month = gcb.month
    AND GetVins.year = gcb.year
    LEFT JOIN RebatesAndDiscounts AS rad ON GetVins.month = rad.month
    AND GetVins.year = rad.year
    LEFT JOIN FundingClerks AS fc ON GetVins.month = fc.month
    AND GetVins.year = fc.year
    LEFT JOIN Commission AS com ON GetVins.month = com.month
    AND GetVins.year = com.year
    LEFT JOIN SalesPersonGuarantee AS spg ON GetVins.month = spg.month
    AND GetVins.year = spg.year
    LEFT JOIN ICPayoffTeam AS icpt ON GetVins.month = icpt.month
    AND GetVins.year = icpt.year
    LEFT JOIN OutboundCommision AS oc ON GetVins.month = oc.month
    AND GetVins.year = oc.year
    LEFT JOIN TitleClerks AS tc ON GetVins.month = tc.month
    AND GetVins.year = tc.year
    LEFT JOIN DirectEMPBenefits AS dempb ON GetVins.month = dempb.month
    AND GetVins.year = dempb.year
    LEFT JOIN DirectPayrollTax AS dpt ON GetVins.month = dpt.month
    AND GetVins.year = dpt.year    
    LEFT JOIN Postage AS postage ON GetVins.month = postage.month
    AND GetVins.year = postage.year
ORDER BY
    COALESCE(GetVins.year) ASC,
    COALESCE(GetVins.month) ASC"""
) 
#AverageAccounts_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Vin Accounts - Getting missing vin totals by month

# COMMAND ----------

MissingVinAccountMissingVins_df = spark.sql(f"""WITH GetVins AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        MAX(so.trandate) AS ns_date
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
    ORDER BY
        MONTH(so.trandate)
),
Reserves AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 236
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
DocFee AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 239
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
TitlingFee AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 451
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
RevVSC AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 237
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
VSCBonus AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 479
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
GAPrev AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 238
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
GAPbonus AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(so.trandate) AS month,
        YEAR(so.trandate) AS year,
        SUM(amount) AS total_amount
    FROM
        bronze.ns.salesinvoiced AS so
        INNER JOIN bronze.ns.transaction AS t ON so.transaction = t.id
    WHERE
        account = 480
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(so.trandate),
        YEAR(so.trandate)
),
PayoffVariance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 532
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
SalesTaxVariance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 533
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
RegistrationVariance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 534
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
CustomerExperience AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 538
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
Penalties AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 539
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
TitlingFeeCor AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 452
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
BankBuyoutFees AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 447
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
        AND t.abbrevtype != 'PURCHORD'
        AND t.abbrevtype != 'SALESORD'
        AND t.abbrevtype != 'RTN AUTH'
        AND (t.approvalstatus = 2 or t.approvalstatus is null)
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
CORvsc AS (
    SELECT 
        'Missing Vin' AS vins,
        month(t.trandate) AS month,
        year(t.trandate) as Year,
        SUM(tl.foreignamount) AS total_amount       
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE        
        tl.expenseaccount = 267
        and t.abbrevtype != 'PURCHORD'
        and (t.approvalstatus = 2 or t.approvalstatus is null)
        and (t.custbody_leaseend_vinno is null or length(t.custbody_leaseend_vinno) != 17)
    group by
        month(t.trandate),
        year(t.trandate)
),
CORVSCadvance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 498
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
CORgap AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 269
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
        AND t.abbrevtype != 'PURCHORD'
        AND t.abbrevtype != 'SALESORD'
        AND t.abbrevtype != 'RTN AUTH'
        AND t.abbrevtype != 'VENDAUTH'
        AND (t.approvalstatus = 2 or t.approvalstatus is null)
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
CORadvance AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 499
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
),
Repo AS (
    SELECT
        'Missing Vin' AS vins,
        MONTH(t.trandate) AS month,
        YEAR(t.trandate) AS year,
        SUM(tl.foreignamount) AS total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 551
        AND (
            LENGTH(t.custbody_leaseend_vinno) != 17
            OR TRIM(t.custbody_leaseend_vinno) = ''
            OR t.custbody_leaseend_vinno IS NULL
        )
    GROUP BY
        MONTH(t.trandate),
        YEAR(t.trandate)
)
SELECT
    GetVins.ns_date,
    GetVins.month,
    GetVins.year,
    CONCAT(
        GetVins.month,
        '-',
        GetVins.year,
        '-',
        GetVins.vins
    ) AS vin,
    COALESCE(r.total_amount, 0) as `4105_rev_reserve`,
    COALESCE(df.total_amount, 0) as `doc_fees_rev_4130`,
    COALESCE(tf.total_amount, 0) as `titling_fees_rev_4141`,
    COALESCE(rv.total_amount, 0) as `vsc_rev_4110`,
    COALESCE(vb.total_amount, 0) as `vsc_volume_bonus_rev_4110b`,
    COALESCE(gr.total_amount, 0) as `gap_rev_4120`,
    COALESCE(gb.total_amount, 0) as `gap_volume_bonus_rev_4120b`,
    COALESCE(pov.total_amount, 0) as `payoff_variance_5400`,
    COALESCE(stv.total_amount, 0) as `sales_tax_variance_5401`,    
    COALESCE(rvar.total_amount, 0) as `registration_variance_5402`,    
    COALESCE(ce.total_amount, 0) as `customer_experience_5403`,
    COALESCE(p.total_amount, 0) as `penalties_5404`,
    COALESCE(tfc.total_amount, 0) as `titling_fees_5141`,
    COALESCE(bbf.total_amount, 0) as `bank_buyout_fees_5520`,
    COALESCE(cvsc.total_amount, 0) as `vsc_cor_5110`,
    COALESCE(cvsca.total_amount, 0) as `vsc_advance_5110a`,
    COALESCE(cg.total_amount, 0) as `gap_cor_5120`,
    COALESCE(ca.total_amount, 0) as `gap_advance_5120a`,
    COALESCE(repo.total_amount, 0) as `repo`
FROM
    GetVins
    LEFT JOIN Reserves AS r ON GetVins.month = r.month AND GetVins.year = r.year
    LEFT JOIN DocFee AS df ON GetVins.month = df.month AND GetVins.year = df.year
    LEFT JOIN TitlingFee tf ON GetVins.month = tf.month AND GetVins.year = tf.year
    LEFT JOIN RevVSC rv ON GetVins.month = rv.month AND GetVins.year = rv.year
    LEFT JOIN VSCBonus vb ON GetVins.month = vb.month AND GetVins.year = vb.year
    LEFT JOIN GAPrev gr ON GetVins.month = gr.month AND GetVins.year = gr.year
    LEFT JOIN GAPbonus gb ON GetVins.month = gb.month AND GetVins.year = gb.year
    LEFT JOIN PayoffVariance pov ON GetVins.month = pov.month AND GetVins.year = pov.year
    LEFT JOIN SalesTaxVariance stv ON GetVins.month = stv.month AND GetVins.year = stv.year
    LEFT JOIN RegistrationVariance rvar ON GetVins.month = rvar.month AND GetVins.year = rvar.year
    LEFT JOIN CustomerExperience ce ON GetVins.month = ce.month AND GetVins.year = ce.year
    LEFT JOIN Penalties p ON GetVins.month = p.month AND GetVins.year = p.year
    LEFT JOIN TitlingFeeCor tfc ON GetVins.month = tfc.month AND GetVins.year = tfc.year
    LEFT JOIN BankBuyoutFees bbf ON GetVins.month = bbf.month AND GetVins.year = bbf.year
    LEFT JOIN CORvsc cvsc ON GetVins.month = cvsc.month AND GetVins.year = cvsc.year
    LEFT JOIN CORVSCadvance cvsca ON GetVins.month = cvsca.month AND GetVins.year = cvsca.year
    LEFT JOIN CORgap cg ON GetVins.month = cg.month AND GetVins.year = cg.year
    LEFT JOIN CORadvance ca ON GetVins.month = ca.month AND GetVins.year = ca.year
    LEFT JOIN Repo as repo ON GetVins.month = repo.month AND GetVins.year = repo.year
ORDER BY
    GetVins.year,
    GetVins.month;""")

#MissingVinAccountMissingVins_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Get Vins from DD
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#get the data entry error vins

vins_from_dd = spark.sql(f"""select vin from bronze.leaseend_db_public.cars """)

# COMMAND ----------

# MAGIC %md
# MAGIC Get Vins that are not in DD (dataentry errors) from the netsuite set

# COMMAND ----------


#Get the data data entry errors from vins and averages

vins_from_dd.createOrReplaceTempView('vins_from_dd')
PerVinSelectedAccounts_df.createOrReplaceTempView('PerVinSelectedAccounts_df')

data_entry_errors_df = spark.sql(f"""SELECT *
FROM PerVinSelectedAccounts_df
LEFT JOIN vins_from_dd ON LOWER(vins_from_dd.vin) = LOWER(PerVinSelectedAccounts_df.vins)
WHERE vins_from_dd.vin IS NULL;
""")

#data_entry_errors_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Sum the data entry errors by month

# COMMAND ----------

#Group the accounts by month and year before you join it to the missing vins data

data_entry_errors_df.createOrReplaceTempView('data_entry_errors_df')

data_entry_errors_groupedby_df = spark.sql(f"""SELECT
    month,
    year,
    SUM(4105_rev_reserve) as 4105_rev_reserve,
    SUM(doc_fees_rev_4130) as doc_fees_rev_4130,
    SUM(titling_fees_rev_4141) as titling_fees_rev_4141,
    SUM(vsc_rev_4110) as vsc_rev_4110,
    SUM(vsc_volume_bonus_rev_4110b) as vsc_volume_bonus_rev_4110b,
    SUM(gap_rev_4120) as gap_rev_4120,
    SUM(gap_volume_bonus_rev_4120b) as gap_volume_bonus_rev_4120b,
    SUM(payoff_variance_5400) as payoff_variance_5400,
    SUM(sales_tax_variance_5401) as sales_tax_variance_5401,
    SUM(registration_variance_5402) as registration_variance_5402,
    SUM(customer_experience_5403) as customer_experience_5403,
    SUM(penalties_5404) as penalties_5404,
    SUM(titling_fees_5141) as titling_fees_5141,
    SUM(bank_buyout_fees_5520) as bank_buyout_fees_5520,
    SUM(vsc_cor_5110) as vsc_cor_5110,
    SUM(vsc_advance_5110a) as vsc_advance_5110a,
    SUM(gap_cor_5120) as gap_cor_5120,
    SUM(gap_advance_5120a) as gap_advance_5120a,
    SUM(repo) as repo
FROM
    data_entry_errors_df
GROUP BY
    year,
    month""")

#data_entry_errors_groupedby_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Add the data entry error amounts to the missing vin data

# COMMAND ----------

#add the data entry errors to the missing vin

from pyspark.sql import SparkSession

data_entry_errors_groupedby_df.createOrReplaceTempView("data_entry_errors_groupedby_df")
MissingVinAccountMissingVins_df.createOrReplaceTempView("MissingVinAccountMissingVins_df")

# Perform a SQL query to join the DataFrames and calculate the sum
MissingRowByMonth_df = spark.sql("""
    SELECT 
        m.month,
        m.year,
        m.vin as vins,
        m.4105_rev_reserve + COALESCE(e.4105_rev_reserve, 0) AS 4105_rev_reserve,
        m.doc_fees_rev_4130 + COALESCE(e.doc_fees_rev_4130, 0) AS doc_fees_rev_4130,
        m.titling_fees_rev_4141 + COALESCE(e.titling_fees_rev_4141, 0) AS titling_fees_rev_4141,
        m.vsc_rev_4110 + COALESCE(e.vsc_rev_4110, 0) AS vsc_rev_4110,
        m.vsc_volume_bonus_rev_4110b + COALESCE(e.vsc_volume_bonus_rev_4110b, 0) AS vsc_volume_bonus_rev_4110b,
        m.gap_rev_4120 + COALESCE(e.gap_rev_4120, 0) AS gap_rev_4120,
        m.gap_volume_bonus_rev_4120b + COALESCE(e.gap_volume_bonus_rev_4120b, 0) AS gap_volume_bonus_rev_4120b,
        m.payoff_variance_5400 + COALESCE(e.payoff_variance_5400, 0) AS payoff_variance_5400,
        m.sales_tax_variance_5401 + COALESCE(e.sales_tax_variance_5401, 0) AS sales_tax_variance_5401,
        m.registration_variance_5402 + COALESCE(e.registration_variance_5402, 0) AS registration_variance_5402,
        m.customer_experience_5403 + COALESCE(e.customer_experience_5403, 0) AS customer_experience_5403,
        m.penalties_5404 + COALESCE(e.penalties_5404, 0) AS penalties_5404,
        m.titling_fees_5141 + COALESCE(e.titling_fees_5141, 0) AS titling_fees_5141,
        m.bank_buyout_fees_5520 + COALESCE(e.bank_buyout_fees_5520, 0) AS bank_buyout_fees_5520,
        m.vsc_cor_5110 + COALESCE(e.vsc_cor_5110, 0) AS vsc_cor_5110,
        m.vsc_advance_5110a + COALESCE(e.vsc_advance_5110a, 0) AS vsc_advance_5110a,
        m.gap_cor_5120 + COALESCE(e.gap_cor_5120, 0) AS gap_cor_5120,
        m.gap_advance_5120a + COALESCE(e.gap_advance_5120a, 0) AS gap_advance_5120a,
        m.repo + COALESCE(e.repo, 0) AS repo
    FROM 
        MissingVinAccountMissingVins_df m
    LEFT JOIN 
        data_entry_errors_groupedby_df e
    ON 
        m.month = e.month 
    AND 
        m.year = e.year
    ORDER BY
         m.year, m.month
""")



# COMMAND ----------

# MAGIC %md
# MAGIC Add the missing vin amounts to the known vin accounts
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, StringType, TimestampType, DateType, LongType

#add ns_date column so number of columns match
MissingRowByMonth_df = MissingRowByMonth_df.withColumn("ns_date", lit(None).cast("timestamp"))

#align the columns before doing a union
MissingRowByMonth_df = MissingRowByMonth_df.select(PerVinSelectedAccounts_df.columns)

vins_and_missing_accounts_df = PerVinSelectedAccounts_df.unionAll(MissingRowByMonth_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Remove the data entry errors from the vin accounts (because these vins are amounts are now part of the misssing vins)

# COMMAND ----------

from pyspark.sql.functions import col

# Get the vins from data_entry_errors_df
vins_to_remove = data_entry_errors_df.select("vins").distinct()

# Remove the rows in formulated_df where vins equals any vin in vins_to_remove
vins_and_missing_accounts_df = vins_and_missing_accounts_df.join(
    vins_to_remove,
    vins_and_missing_accounts_df.vins == vins_to_remove.vins,
    "left_anti"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Average amounts are off because the drivers vs the actual driver (drivers - missing vins) so we need to take the total amount on averages and divide by the actual drivers to make sure the amounts are accurate
# MAGIC
# MAGIC Also we are joining average accounts to the vin accounts

# COMMAND ----------

from pyspark.sql.functions import col, when

# Get the count of rows by month and year on vins_andMissingaccounts_df, ordered by year then month
actualDrivers_df = vins_and_missing_accounts_df.groupby("year", "month").count().orderBy("year", "month")

# Rename the count column to valid_drivers
actualDrivers_df = actualDrivers_df.withColumnRenamed("count", "valid_drivers")

# Join actualDrivers_df with AverageAccounts_df based on month and year
actualDrivers_df = actualDrivers_df.join(AverageAccounts_df, (actualDrivers_df.month == AverageAccounts_df.avg_month) & (actualDrivers_df.year == AverageAccounts_df.avg_year), "inner")

# Select the necessary columns
AverageAccounts_with_actuals = actualDrivers_df.select(AverageAccounts_df.columns + [actualDrivers_df.valid_drivers])


AverageAccounts_with_actuals = AverageAccounts_with_actuals \
    .withColumn("reserve_bonus_rev_4106", when(col("4106") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4106")).otherwise(col("4106") / col("valid_drivers")))) \
    .withColumn("reserve_chargeback_rev_4107", when(col("4107") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4107")).otherwise(col("4107") / col("valid_drivers")))) \
    .withColumn("doc_fees_chargeback_rev_4130c", when(col("4130c") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4130c")).otherwise(col("4130c") / col("valid_drivers")))) \
    .withColumn("vsc_advance_rev_4110a", when(col("4110a") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4110a")).otherwise(col("4110a") / col("valid_drivers")))) \
    .withColumn("vsc_cost_rev_4110c", when(col("4110c") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4110c")).otherwise(col("4110c") / col("valid_drivers")))) \
    .withColumn("vsc_chargeback_rev_4111", when(col("4111") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4111")).otherwise(col("4111") / col("valid_drivers")))) \
    .withColumn("gap_advance_rev_4120a", when(col("4120a") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4120a")).otherwise(col("4120a") / col("valid_drivers")))) \
    .withColumn("gap_cost_rev_4120c", when(col("4120c") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4120c")).otherwise(col("4120c") / col("valid_drivers")))) \
    .withColumn("gap_chargeback_rev_4121", when(col("4121") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4121")).otherwise(col("4121") / col("valid_drivers")))) \
    .withColumn("4190_average", when(col("4190") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("4190")).otherwise(col("4190") / col("valid_drivers")))) \
    .withColumn("funding_clerks_5301", when(col("5301") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5301")).otherwise(col("5301") / col("valid_drivers")))) \
    .withColumn("commission_5302", when(col("5302") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5302")).otherwise(col("5302") / col("valid_drivers")))) \
    .withColumn("sales_guarantee_5303", when(col("5303") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5303")).otherwise(col("5303") / col("valid_drivers")))) \
    .withColumn("ic_payoff_team_5304", when(col("5304") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5304")).otherwise(col("5304") / col("valid_drivers")))) \
    .withColumn("outbound_commission_5305", when(col("5305") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5305")).otherwise(col("5305") / col("valid_drivers")))) \
    .withColumn("title_clerks_5320", when(col("5320") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5320")).otherwise(col("5320") / col("valid_drivers")))) \
    .withColumn("direct_emp_benefits_5330", when(col("5330") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5330")).otherwise(col("5330") / col("valid_drivers")))) \
    .withColumn("direct_payroll_tax_5340", when(col("5340") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5340")).otherwise(col("5340") / col("valid_drivers")))) \
    .withColumn("postage_5510", when(col("5510") == 0, 0).otherwise(when(col("valid_drivers") == 1, col("5510")).otherwise(col("5510") / col("valid_drivers"))))

totals_df = vins_and_missing_accounts_df.join(AverageAccounts_with_actuals, (vins_and_missing_accounts_df.year == AverageAccounts_with_actuals.avg_year) & (vins_and_missing_accounts_df.month == AverageAccounts_with_actuals.avg_month), 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC Add source from DD for account requirements.  5302, 5303, 5305
# MAGIC

# COMMAND ----------

#add the source from dd, some vins have more than 1 deal so the source can make duplicates

totals_df.createOrReplaceTempView('totals_df')

vin_source_df = spark.sql("""
    SELECT DISTINCT totals_df.vins, d.source, totals_df.*
    FROM totals_df
    LEFT JOIN bronze.leaseend_db_public.cars c ON lower(totals_df.vins) = lower(c.vin)
    LEFT JOIN bronze.leaseend_db_public.deals d ON c.deal_id = d.id;
""")
vin_source_df = vin_source_df.dropDuplicates(subset=['vins'])

#vin_source_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC 5302, 5303, 5305, 5330, 5340 totals by month

# COMMAND ----------

source_accounts_df = spark.sql(f"""WITH 
CORCOM AS (                     
    SELECT
        YEAR(t.trandate) AS year,
        MONTH(t.trandate) AS month,
        SUM(tl.foreignamount) AS 5302_total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 552
    GROUP BY
        YEAR(t.trandate),
        MONTH(t.trandate)
),
CORSALE AS (                     
    SELECT
        YEAR(t.trandate) AS year,
        MONTH(t.trandate) AS month,
        SUM(tl.foreignamount) AS 5303_total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 553
    GROUP BY
        YEAR(t.trandate),
        MONTH(t.trandate)
),
OUTCOM AS (
    SELECT
        YEAR(t.trandate) AS year,
        MONTH(t.trandate) AS month,
        SUM(tl.foreignamount) AS 5305_total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 564
    GROUP BY
        YEAR(t.trandate),
        MONTH(t.trandate)
),
EMP AS (
    SELECT
        YEAR(t.trandate) AS year,
        MONTH(t.trandate) AS month,
        SUM(tl.foreignamount) AS 5330_total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 530
    GROUP BY
        YEAR(t.trandate),
        MONTH(t.trandate)
),
PAY AS (
    SELECT
        YEAR(t.trandate) AS year,
        MONTH(t.trandate) AS month,
        SUM(tl.foreignamount) AS 5340_total_amount
    FROM
        bronze.ns.transactionline AS tl
        INNER JOIN bronze.ns.transaction AS t ON tl.transaction = t.id
    WHERE
        tl.expenseaccount = 531
    GROUP BY
        YEAR(t.trandate),
        MONTH(t.trandate)
)
SELECT
    CORCOM.month as smonth,
    CORCOM.year as syear,
    5302_total_amount,
    5303_total_amount,
    5305_total_amount,
    5330_total_amount,
    5340_total_amount
FROM
    CORCOM
    LEFT JOIN CORSALE AS sales ON CORCOM.month = sales.month AND CORCOM.year = sales.year
    LEFT JOIN OUTCOM AS outbound ON CORCOM.month = outbound.month AND CORCOM.year = outbound.year
    LEFT JOIN EMP AS direct ON CORCOM.month = direct.month AND CORCOM.year = direct.year
    LEFT JOIN PAY AS tax ON CORCOM.month = tax.month AND CORCOM.year = tax.year
""")


#source_accounts_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC web and call-in deal count by year and month and outbound by year and month
# MAGIC

# COMMAND ----------

#web and call-in
filtered_web_callin_df = vin_source_df.filter((vin_source_df['source'] == 'call-in') | (vin_source_df['source'] == 'web'))
grouped_web_callin_df = filtered_web_callin_df.groupBy('month', 'year')
count_web_callin_df = grouped_web_callin_df.count()
count_web_callin_df = count_web_callin_df.withColumnRenamed('count', 'web_callin_count')

#count_web_callin_df.orderBy('year', 'month').display()

#outbound
filtered_outbound_df = vin_source_df.filter((vin_source_df['source'] == 'outbound'))
grouped_outbound_df = filtered_outbound_df.groupBy('month', 'year')
count_outbound_df = grouped_outbound_df.count()
count_outbound_df = count_outbound_df.withColumnRenamed('count', 'outbound_count')

#count_outbound_df.display()

# not d2d not used
#filtered_notd2d_df = vin_source_df.filter((vin_source_df['source'] == 'outbound') | (vin_source_df['source'] == 'call-in') | (vin_source_df['source'] == 'web'))
#grouped_not2d2_df = filtered_notd2d_df.groupBy('month', 'year')
#count_notd2d_df = grouped_not2d2_df.count()
#count_notd2d_df = count_notd2d_df.withColumnRenamed('count', 'notd2d_count')

filtered_inbound_outbound_df = vin_source_df.filter((vin_source_df['source'] == 'outbound') | (vin_source_df['source'] == 'call-in') | (vin_source_df['source'] == 'web'))
grouped_inbound_outbound_df = filtered_inbound_outbound_df.groupBy('month', 'year')
count_inbound_outbound_df = grouped_inbound_outbound_df.count()
count_inbound_outbound_df = count_inbound_outbound_df.withColumnRenamed('count', 'inbound_outbound_count')

#count_inbound_outbound_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC join web and call-in & outbound source types counts by month and year to amounts by month and year 

# COMMAND ----------

source_accounts_and_counts_df = source_accounts_df.join(count_web_callin_df, (source_accounts_df.smonth == count_web_callin_df.month ) & (source_accounts_df.syear == count_web_callin_df.year), 'left')
source_accounts_and_counts_df = source_accounts_and_counts_df.drop('month', 'year')
source_accounts_and_counts_df = source_accounts_and_counts_df.join(count_outbound_df, (source_accounts_and_counts_df.smonth == count_outbound_df.month ) & (source_accounts_and_counts_df.syear == count_outbound_df.year), 'left')
source_accounts_and_counts_df = source_accounts_and_counts_df.drop('month', 'year')
source_accounts_and_counts_df = source_accounts_and_counts_df.join(count_inbound_outbound_df, (source_accounts_and_counts_df.smonth == count_inbound_outbound_df.month ) & (source_accounts_and_counts_df.syear == count_inbound_outbound_df.year), 'left')
source_accounts_and_counts_df = source_accounts_and_counts_df.drop('month', 'year')

#source_accounts_and_counts_df.display()
#source_accounts_and_counts_df.select('year', 'month', 'web_callin_count', '5302_total_amount', '5303_total_amount').orderBy('year', 'month').display()

# COMMAND ----------

# MAGIC %md
# MAGIC get the averges by month and year for 5302, 5303, & 5305, 5330, 5340 accounts 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when

# Calculate the averages in a single withColumn call
count_source_averages_df = (
    source_accounts_and_counts_df
    .withColumn('5302_average',
                when(col('web_callin_count').isNull(), col('5302_total_amount'))
                .otherwise(col('5302_total_amount') / col('web_callin_count')))
    .withColumn('5303_average',
                when(col('web_callin_count').isNull(), col('5303_total_amount'))
                .otherwise(col('5303_total_amount') / col('web_callin_count')))
    .withColumn('5305_average',
                when(col('outbound_count').isNull(), col('5305_total_amount'))
                .otherwise(col('5305_total_amount') / col('outbound_count')))
    .withColumn('5330_average',
                when(col('inbound_outbound_count').isNull(), col('5330_total_amount'))
                .otherwise(col('5330_total_amount') / col('inbound_outbound_count')))
    .withColumn('5340_average',
                when(col('inbound_outbound_count').isNull(), col('5340_total_amount'))
                .otherwise(col('5340_total_amount') / col('inbound_outbound_count')))
)

#count_source_averages_df.display()
averages_df = count_source_averages_df #.drop('5302_total_amount', '5303_total_amount', '5305_total_amount', '5330_total_amount', '5330_total_amount', '5340_total_amount', 'web_callin_count', 'outbound_count', 'notd2d_count')


# COMMAND ----------

# MAGIC %md
# MAGIC update the 5302, 5303, and 5305, 5330, 5340 columns with the new averages based on source

# COMMAND ----------

from pyspark.sql.functions import col, when

# Perform the join operation
vin_source_averages_df = vin_source_df.join(
    averages_df,
    (vin_source_df['month'] == averages_df['smonth']) & (vin_source_df['year'] == averages_df['syear']),
    'left'
)

# Define conditions for commission calculations
condition_commission = (vin_source_averages_df['source'] == 'call-in') | (vin_source_averages_df['source'] == 'web') | (vin_source_averages_df['source'].isNull())
condition_sales_guarantee = condition_commission
condition_outbound_commission = (vin_source_averages_df['source'] == 'outbound') | (vin_source_averages_df['source'].isNull())
condition_direct_benefits = (vin_source_averages_df['source'] == 'call-in') | (vin_source_averages_df['source'] == 'outbound') | (vin_source_averages_df['source'] == 'web') | (vin_source_averages_df['source'].isNull())
condition_direct_payroll_tax = condition_direct_benefits

# Perform calculations using when and otherwise
vin_source_averages_df = vin_source_averages_df.withColumn(
    'commission_5302',
    when(condition_commission, col('5302_average')).otherwise(0)
).withColumn(
    'sales_guarantee_5303',
    when(condition_sales_guarantee, col('5303_average')).otherwise(0)
).withColumn(
    'outbound_commission_5305',
    when(condition_outbound_commission, col('5305_average')).otherwise(0)
).withColumn(
    'direct_emp_benefits_5330',
    when(condition_direct_benefits, col('5330_average')).otherwise(0)
).withColumn(
    'direct_payroll_tax_5340',
    when(condition_direct_payroll_tax, col('5340_average')).otherwise(0)
)

#vin_source_averages_df.select('month', 'year', 'vins', 'source', '5330', '5330_average', 'direct_emp_benefits_5330', '5340', '5340_average', 'direct_payroll_tax_5340').display()
#vin_source_averages_df.select('month', 'year', 'source', 'inbound_outbound_count', 'direct_payroll_tax_5340', '5340_total_amount', '5340_average', 'direct_payroll_tax_53401111', ).display()


# COMMAND ----------

# MAGIC %md
# MAGIC clean up and format data, also add new columns for totals (sum of accounts) and select columns in order

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.types import FloatType

formulated_df = vin_source_averages_df.withColumn(
        "reserve_total_rev",
        coalesce(col("4105_rev_reserve").cast(FloatType()), lit(0)) + coalesce(col("reserve_bonus_rev_4106").cast(FloatType()), lit(0)) + coalesce(col("reserve_chargeback_rev_4107").cast(FloatType()), lit(0))
    ).withColumn(
        "vsc_total_rev",
        coalesce(col("vsc_rev_4110").cast(FloatType()), lit(0)) + coalesce(col("vsc_advance_rev_4110a").cast(FloatType()), lit(0)) + coalesce(col("vsc_volume_bonus_rev_4110b").cast(FloatType()), lit(0)) + coalesce(col("vsc_cost_rev_4110c").cast(FloatType()), lit(0)) + coalesce(col("vsc_chargeback_rev_4111").cast(FloatType()), lit(0))
    ).withColumn(
        "gap_total_rev",
        coalesce(col("gap_rev_4120").cast(FloatType()), lit(0)) + coalesce(col("gap_advance_rev_4120a").cast(FloatType()), lit(0)) + coalesce(col("gap_volume_bonus_rev_4120b").cast(FloatType()), lit(0)) + coalesce(col("gap_cost_rev_4120c").cast(FloatType()), lit(0)) + coalesce(col("gap_chargeback_rev_4121").cast(FloatType()), lit(0))
    ).withColumn(
        "doc_&_title_total_rev",
        coalesce(col("doc_fees_rev_4130").cast(FloatType()), lit(0)) + coalesce(col("doc_fees_chargeback_rev_4130c").cast(FloatType()), lit(0)) + coalesce(col("titling_fees_rev_4141").cast(FloatType()), lit(0))
    ).withColumn(
        "total_revenue",
        coalesce(col("reserve_total_rev").cast(FloatType()), lit(0)) + coalesce(col("vsc_total_rev").cast(FloatType()), lit(0)) + coalesce(col("gap_total_rev").cast(FloatType()), lit(0)) + coalesce(col("doc_&_title_total_rev").cast(FloatType()), lit(0))
    ).withColumn(
        "direct_people_cost",
        coalesce(col("funding_clerks_5301").cast(FloatType()), lit(0)) + coalesce(col("commission_5302").cast(FloatType()), lit(0)) + coalesce(col("sales_guarantee_5303").cast(FloatType()), lit(0)) + coalesce(col("ic_payoff_team_5304").cast(FloatType()), lit(0)) + coalesce(col("outbound_commission_5305").cast(FloatType()), lit(0)) + coalesce(col("title_clerks_5320").cast(FloatType()), lit(0)) + coalesce(col("direct_emp_benefits_5330").cast(FloatType()), lit(0)) + coalesce(col("direct_payroll_tax_5340").cast(FloatType()), lit(0))
    ).withColumn(
        "payoff_variance_total",
        coalesce(col("payoff_variance_5400").cast(FloatType()), lit(0)) + coalesce(col("sales_tax_variance_5401").cast(FloatType()), lit(0)) + coalesce(col("registration_variance_5402").cast(FloatType()), lit(0)) + coalesce(col("customer_experience_5403").cast(FloatType()), lit(0)) + coalesce(col("penalties_5404").cast(FloatType()), lit(0))
    ).withColumn(
        "other_cor_total",
        coalesce(col("postage_5510").cast(FloatType()), lit(0)) + coalesce(col("bank_buyout_fees_5520").cast(FloatType()), lit(0))
    ).withColumn(
        "vsc_total",
        coalesce(col("vsc_cor_5110").cast(FloatType()), lit(0)) + coalesce(col("vsc_advance_5110a").cast(FloatType()), lit(0))
    ).withColumn(
        "gap_total",
        coalesce(col("gap_cor_5120").cast(FloatType()), lit(0)) + coalesce(col("gap_advance_5120a").cast(FloatType()), lit(0))
    ).withColumn(
        "cor_total",
        coalesce(col("other_cor_total").cast(FloatType()), lit(0)) + coalesce(col("direct_people_cost").cast(FloatType()), lit(0)) + coalesce(col("payoff_variance_total").cast(FloatType()), lit(0))  + coalesce(col("vsc_total").cast(FloatType()), lit(0)) + coalesce(col("gap_total").cast(FloatType()), lit(0))
    ).withColumn(
        "gross_profit",
        coalesce(col("total_revenue").cast(FloatType()), lit(0)) - coalesce(col("cor_total").cast(FloatType()), lit(0))
    ).withColumn(
        "gross_margin",
        coalesce(col("gross_profit").cast(FloatType()), lit(0)) / coalesce(col("total_revenue").cast(FloatType()), lit(0)) * 100
    )


# COMMAND ----------

# Reorder the columns
ns_df = formulated_df.select(
        "month",
        "year",
        "ns_date",
        "vins",
        "4105_rev_reserve",
        "reserve_bonus_rev_4106",
        "reserve_chargeback_rev_4107",
        "reserve_total_rev",
        "vsc_rev_4110",
        "vsc_advance_rev_4110a",
        "vsc_volume_bonus_rev_4110b",
        "vsc_cost_rev_4110c",
        "vsc_chargeback_rev_4111",
        "vsc_total_rev",
        "gap_rev_4120",
        "gap_advance_rev_4120a",
        "gap_volume_bonus_rev_4120b",
        "gap_cost_rev_4120c",
        "gap_chargeback_rev_4121",
        "gap_total_rev",
        "doc_fees_rev_4130",
        "doc_fees_chargeback_rev_4130c",
        "titling_fees_rev_4141",
        "doc_&_title_total_rev",
        "total_revenue",
        "funding_clerks_5301",
        "commission_5302",
        "sales_guarantee_5303",
        "ic_payoff_team_5304",
        "outbound_commission_5305",
        "title_clerks_5320",
        "direct_emp_benefits_5330",
        "direct_payroll_tax_5340",
        "direct_people_cost",
        "payoff_variance_5400",
        "sales_tax_variance_5401",
        "registration_variance_5402",
        "customer_experience_5403",
        "penalties_5404",
        "payoff_variance_total",
        "postage_5510",
        "bank_buyout_fees_5520",
        "other_cor_total",
        "vsc_cor_5110",
        "vsc_advance_5110a",
        "vsc_total",
        "gap_cor_5120",
        "gap_advance_5120a",
        "gap_total",
        "cor_total",
        "gross_profit",
        "gross_margin",
        "repo"
    ) 

# COMMAND ----------

# MAGIC %md
# MAGIC Accounting wants the following 4000 accounts to be the same values as the following 5000 accounts <br>
# MAGIC 4110a (546) should be 5110a (498) <br>
# MAGIC 4110c (545) should be 5110 (267) <br>
# MAGIC 4120a (547) should be 5120a (499) <br>
# MAGIC 4120c (548) should be 5120 (269)

# COMMAND ----------

from pyspark.sql.functions import col

ns_df = ns_df.withColumn('vsc_advance_rev_4110a', col('vsc_advance_5110a'))
ns_df = ns_df.withColumn('vsc_cost_rev_4110c', col('vsc_cor_5110'))
ns_df = ns_df.withColumn('gap_advance_rev_4120a', col('gap_advance_5120a'))
ns_df = ns_df.withColumn('gap_cost_rev_4120c', col('gap_cor_5120'))

# COMMAND ----------

#ns_df.write.format("delta").mode("overwrite").saveAsTable("silver.deal.big_deal_ns")

# COMMAND ----------

#ns_df.display()