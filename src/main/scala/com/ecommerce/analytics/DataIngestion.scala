package com.ecommerce.analytics

import com.ecommerce.models._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class DataIngestion(spark: SparkSession) {
  import spark.implicits._

  // transactions.csv 
  private val txSchema = StructType(Seq(
    StructField("transaction_id", StringType),
    StructField("user_id",        StringType),
    StructField("product_id",     StringType),
    StructField("merchant_id",    StringType),
    StructField("amount",         DoubleType),
    StructField("timestamp",      StringType),   
    StructField("location",       StringType),
    StructField("payment_method", StringType),
    StructField("category",       StringType)
  ))
  def readTransactions(path: String): Dataset[Transaction] =
    spark.read.option("header","true").schema(txSchema).csv(path).as[Transaction]

  // users.json 
   def readUsers(path: String): Dataset[User] =
    spark.read.json(path)
      .withColumn("age", col("age").cast(IntegerType))
      .as[User]

  // products.parquet 
  def readProducts(path: String): Dataset[Product] =
    spark.read.parquet(path).as[Product]

  // merchants.csv 
  def readMerchants(path: String): Dataset[Merchant] =
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)
      .withColumn("establishment_date", col("establishment_date").cast(StringType))
      .as[Merchant]
}