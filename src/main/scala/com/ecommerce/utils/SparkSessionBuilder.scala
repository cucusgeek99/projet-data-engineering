package com.ecommerce.utils
import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def build(): SparkSession = {
    val spark = SparkSession.builder()
      .appName(ConfigLoader.getString("app.name", "EcommerceAnalytics"))
      .master(ConfigLoader.getString("app.spark.master", "local[*]"))
      .config("spark.sql.shuffle.partitions",
              ConfigLoader.getInt("app.spark.shuffle.partitions", 8))
      .getOrCreate()
    spark.sparkContext.setLogLevel(ConfigLoader.getString("app.spark.log-level", "WARN"))
    spark
  }
}