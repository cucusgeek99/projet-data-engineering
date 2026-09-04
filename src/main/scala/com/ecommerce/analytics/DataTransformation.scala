package com.ecommerce.analytics

import com.ecommerce.models._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class DataTransformation {

  private val SECONDS_IN_SEVEN_DAYS = 7 * 24 * 60 * 60

  def enrichTransactionData(
      transactions: Dataset[Transaction],
      users: Dataset[User],
      products: Dataset[Product],
      merchants: Dataset[Merchant]
  ): DataFrame = {

    // Sélection des informations utiles sur les utilisateurs

    val usersSelected = users.select(
      col("user_id"), col("age"), col("annual_income"), col("city"),
      col("customer_segment"), col("preferred_categories"), col("registration_date")
    )

    // Sélection et renommage des informations produits
    val productsSelected = products.select(
      col("product_id"),
      col("name").as("product_name"),
      col("category").as("product_category"),
      col("price").as("product_price"),
      col("rating").as("product_rating"),
      col("stock").as("product_stock")
    )

    // Sélection et renommage des informations marchants
    val merchantsSelected = merchants.select(
      col("merchant_id"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region").as("merchant_region"),
      col("commission_rate"),
      col("establishment_date").as("merchant_establishment_date")
    )

    val transactionsWithTs = transactions
      .withColumn("tx_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))

    val joined = transactionsWithTs
      .join(usersSelected, Seq("user_id"), "left")
      .join(productsSelected, Seq("product_id"), "left")
      .join(merchantsSelected, Seq("merchant_id"), "left")

    val withTimeFeatures = joined
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .withColumn("hour", col("time_features.hour"))
      .withColumn("day_of_week", col("time_features.day_of_week"))
      .withColumn("month", col("time_features.month"))
      .withColumn("is_weekend", col("time_features.is_weekend"))
      .withColumn("day_period", col("time_features.day_period"))
      .withColumn("is_working_hours", col("time_features.is_working_hours"))
      .drop("time_features")

    val perUserOrdered = Window.partitionBy("user_id").orderBy("tx_ts")
    val perUserAll = Window.partitionBy("user_id")

    val withUserWindows = withTimeFeatures
      .withColumn("transaction_rank_for_user", row_number().over(perUserOrdered))
      .withColumn("total_transactions_for_user", count("transaction_id").over(perUserAll))

    // création de la colonne Age
    withUserWindows.withColumn(
      "age_bracket",
      when(col("age").isNull, lit(null: String))
        .when(col("age") < 25, "Jeune")
        .when(col("age") >= 25 && col("age") <= 44, "Adulte")
        .when(col("age") >= 45 && col("age") <= 64, "Âge Moyen")
        .otherwise("Senior")
    )
  }
}