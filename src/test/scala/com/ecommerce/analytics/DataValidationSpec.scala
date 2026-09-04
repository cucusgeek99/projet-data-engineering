package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Tests Spark pour les règles de validation (Q2.2). */
class DataValidationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("DataValidationSpec").master("local[2]").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("validateTransactions sépare montants/horodatages invalides, aucune ligne perdue") {
    val s = spark
    import s.implicits._
    val df = Seq(
      ("TX1", 10.0, "20240701093000"), // valide
      ("TX2", -5.0, "20240701093000"), // montant négatif
      ("TX3", 0.0, "20240701093000"),  // montant nul
      ("TX4", 20.0, "2024070109")      // timestamp trop court
    ).toDF("transaction_id", "amount", "timestamp")

    val result = DataValidation.validateTransactions(df)

    assert(result.valid.count() == 1)
    assert(result.rejected.count() == 3)
    assert(result.valid.count() + result.rejected.count() == df.count())
    assert(result.rejected.columns.contains("rejection_reason"))
  }

  test("validateUsers rejette âge et revenu hors bornes") {
    val s = spark
    import s.implicits._
    val df = Seq(
      ("U1", 25, 30000.0),  // valide
      ("U2", 10, 30000.0),  // trop jeune
      ("U3", 25, -100.0)    // revenu invalide
    ).toDF("user_id", "age", "annual_income")

    val result = DataValidation.validateUsers(df)
    assert(result.valid.count() == 1)
    assert(result.rejected.count() == 2)
  }

  test("validateMerchants rejette un taux de commission hors [0,1]") {
    val s = spark
    import s.implicits._
    val df = Seq(
      ("M1", 0.05),  // valide
      ("M2", 1.5),   // hors borne
      ("M3", -0.1)   // hors borne
    ).toDF("merchant_id", "commission_rate")

    val result = DataValidation.validateMerchants(df)
    assert(result.valid.count() == 1)
    assert(result.rejected.count() == 2)
  }
}
