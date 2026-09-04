package com.ecommerce.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class Analytics {

    def merchantKpis(enrichedTransactions: DataFrame): DataFrame = {

        val cached = SparkOptimizations.cacheIfEnabled(enrichedTransactions)

        // KPIs principaux par marchand
        val baseKPIs = cached
            .groupBy("merchant_id", "merchant_name", "merchant_category", "merchant_region")
            .agg(
                sum("amount").as("total_revenue"),
                count("transaction_id").as("total_transactions"),
                countDistinct("user_id").as("unique_customers"),
                avg("amount").as("avg_transaction_amount"),
                sum(col("amount") * col("commission_rate")).as("total_commission")
            )

        // Classement des marchands

        val rankInCategory = Window.
            partitionBy("merchant_category").
            orderBy(col("total_revenue").desc)

        val rankInRegion = Window
            .partitionBy("merchant_region")
            .orderBy(col("total_revenue").desc)

        val ranked = baseKPIs
            .withColumn("rank_in_category", rank().over(rankInCategory))
            .withColumn("rank_in_region", rank().over(rankInRegion))

        // Chiffre d'affaires par tranche d'âge
        val salesByAgeBracket = cached
            .filter(col("age_bracket").isNotNull)
            .groupBy("merchant_id")
            .pivot("age_bracket")
            .agg(sum("amount"))
            .na.fill(0.0)

        ranked.join(salesByAgeBracket, Seq("merchant_id"), "left")
    }
        
        def userCohortAnalysis(enrichedTransactions: DataFrame): DataFrame = {

        val cached = SparkOptimizations.cacheIfEnabled(enrichedTransactions)

        // Mois de première transaction = mois de cohorte
        val firstTxWindow = Window.partitionBy("user_id")

        val withCohort = cached
            .withColumn("tx_month", date_trunc("month", col("tx_ts")))
            .withColumn("cohort_month", min("tx_month").over(firstTxWindow))

        val withPeriodIndex = withCohort
            .withColumn(
                "period_index",
                round(months_between(col("tx_month"), col("cohort_month")), 0).cast("int")
            )

        // Taille initiale de chaque cohorte
        val cohortSizes = withPeriodIndex
            .filter(col("period_index") === 0)
            .groupBy("cohort_month")
            .agg(countDistinct("user_id").as("initial_users"))

        val activeByPeriod = withPeriodIndex
            .groupBy("cohort_month", "period_index")
            .agg(countDistinct("user_id").as("active_users"))

        activeByPeriod
            .join(cohortSizes, Seq("cohort_month"), "left")
            .withColumn("retention_pct", round(col("active_users") / col("initial_users") * 100, 2))
            .orderBy("cohort_month", "period_index")
    }

    def bestCohortAt3Months(retentionMatrix: DataFrame): DataFrame = {
        retentionMatrix
        .filter(col("period_index") === 3)
        .orderBy(col("retention_pct").desc)
        .limit(1)
    }
}