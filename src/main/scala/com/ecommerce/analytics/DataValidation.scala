package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Partie 2 — Q2.2 (validation valides/rejetées) + Q2.4 (rapport de qualité).
  */
object DataValidation {

  /** Lignes conformes + lignes rejetées enrichies d'une colonne `rejection_reason`. */
  final case class ValidationResult(valid: DataFrame, rejected: DataFrame)

  // --- seuils lus depuis application.conf, avec valeurs par défaut ---
  private val minAmount     = ConfigLoader.getDouble("app.validation.transaction.min-amount", 0.0)
  private val tsLength      = ConfigLoader.getInt("app.validation.transaction.timestamp-length", 14)
  private val minAge        = ConfigLoader.getInt("app.validation.user.min-age", 16)
  private val maxAge        = ConfigLoader.getInt("app.validation.user.max-age", 100)
  private val minIncome     = ConfigLoader.getDouble("app.validation.user.min-income", 0.0)
  private val minPrice      = ConfigLoader.getDouble("app.validation.product.min-price", 0.0)
  private val minRating     = ConfigLoader.getDouble("app.validation.product.min-rating", 1.0)
  private val maxRating     = ConfigLoader.getDouble("app.validation.product.max-rating", 5.0)
  private val minCommission = ConfigLoader.getDouble("app.validation.merchant.min-commission", 0.0)
  private val maxCommission = ConfigLoader.getDouble("app.validation.merchant.max-commission", 1.0)

  /** Applique une liste de règles (condition d'ÉCHEC -> libellé) et sépare le DataFrame.
    * `rejection_reason` = tous les libellés des règles violées, séparés par "; ". */
  private def applyRules(df: DataFrame, rules: Seq[(Column, String)]): ValidationResult = {
    // concat_ws ignore automatiquement les valeurs nulles :
    val reasonCols = rules.map { case (failCond, label) => when(failCond, lit(label)) }
    val withReason = df.withColumn("rejection_reason", concat_ws("; ", reasonCols: _*))

    val valid    = withReason.filter(col("rejection_reason") === "").drop("rejection_reason")
    val rejected = withReason.filter(col("rejection_reason") =!= "")
    ValidationResult(valid, rejected)
  }

  // ------------------------------------------------------------------ Q2.2

  def validateTransactions(df: DataFrame): ValidationResult = applyRules(df, Seq(
    (col("amount").isNull || col("amount") <= minAmount,
      s"amount <= $minAmount ou null"),
    (col("timestamp").isNull || length(col("timestamp")) =!= tsLength,
      s"timestamp != $tsLength caracteres")
  ))

  def validateUsers(df: DataFrame): ValidationResult = applyRules(df, Seq(
    (col("age").isNull || col("age") < minAge || col("age") > maxAge,
      s"age hors [$minAge, $maxAge]"),
    (col("annual_income").isNull || col("annual_income") <= minIncome,
      s"annual_income <= $minIncome ou null")
  ))

  def validateProducts(df: DataFrame): ValidationResult = applyRules(df, Seq(
    (col("price").isNull || col("price") <= minPrice,
      s"price <= $minPrice ou null"),
    (col("rating").isNull || col("rating") < minRating || col("rating") > maxRating,
      s"rating hors [$minRating, $maxRating]")
  ))

  def validateMerchants(df: DataFrame): ValidationResult = applyRules(df, Seq(
    (col("commission_rate").isNull
       || col("commission_rate") < minCommission
       || col("commission_rate") > maxCommission,
      s"commission_rate hors [$minCommission, $maxCommission]")
  ))

  // ------------------------------------------------------------------ Q2.4

  /** Nombre total de valeurs nulles, toutes colonnes confondues, pour un DataFrame. */
  def countNulls(df: DataFrame): Long = {
    if (df.columns.isEmpty) return 0L
    val exprs = df.columns.toSeq.map(c => sum(when(col(c).isNull, 1L).otherwise(0L)).alias(c))
    val row   = df.agg(exprs.head, exprs.tail: _*).first()
    (0 until row.length).map(i => if (row.isNullAt(i)) 0L else row.getLong(i)).sum
  }

  /** Rapport de qualité : 1 ligne par dataset, affiché console + sauvé en CSV. */
  def qualityReport(
      spark: SparkSession,
      raw: Map[String, DataFrame],
      results: Map[String, ValidationResult],
      outputPath: String
  ): DataFrame = {
    import spark.implicits._

    val rows = raw.keys.toSeq.sorted.map { name =>
      val nbLues  = raw(name).count()
      val nbValid = results(name).valid.count()
      val nbRej   = results(name).rejected.count()
      val taux =
        if (nbLues == 0) 0.0
        else BigDecimal.valueOf(nbRej.toDouble * 100.0 / nbLues.toDouble)
               .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      val nbNulls = countNulls(raw(name))
      (name, nbLues, nbValid, nbRej, taux, nbNulls)
    }

    val report = rows.toDF(
      "dataset", "nb_lignes_lues", "nb_lignes_valides",
      "nb_lignes_rejetees", "taux_rejet", "nb_valeurs_nulles"
    )

    println("\n=== RAPPORT DE QUALITE DES DONNEES ===")
    report.show(false)

    report.coalesce(1)
      .write.mode("overwrite").option("header", "true")
      .csv(s"$outputPath/quality_report")

    report
  }
}