package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.{ConfigLoader, SparkSessionBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, desc}
import org.apache.spark.sql.types.ArrayType

import scala.util.{Failure, Success, Try}

/** Partie 6 — Q6.1 : object principal orchestrant le pipeline complet.
  *
  * Ingestion  -> Validation -> Transformation  -> Analytique 
  * -> écriture des résultats (CSV + Parquet). SparkSession externalisée (Partie 7),
  * gestion d'erreurs globale garantissant un arrêt propre de la SparkSession.
  *
  */
object MainApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBuilder.build()
    import spark.implicits._

    val outcome: Try[Unit] = Try {
      val outputPath = ConfigLoader.getString("app.data.output.path", "output/")

      // ------------------------------------------------------------ 1. Ingestion 
      val inputPaths = Map(
        "transactions" -> ConfigLoader.getString(
          "app.data.input.transactions", "src/main/resources/data/transactions.csv"),
        "users" -> ConfigLoader.getString(
          "app.data.input.users", "src/main/resources/data/users.json"),
        "products" -> ConfigLoader.getString(
          "app.data.input.products", "src/main/resources/data/products.parquet"),
        "merchants" -> ConfigLoader.getString(
          "app.data.input.merchants", "src/main/resources/data/merchants.csv")
      )

      val raw = new DataIngestion(spark).loadAll(inputPaths)

      // ------------------------------------------------------------ 2. Validation 
      val txResult = DataValidation.validateTransactions(raw.transactions.toDF())
      val usResult = DataValidation.validateUsers(raw.users.toDF())
      val prResult = DataValidation.validateProducts(raw.products.toDF())
      val meResult = DataValidation.validateMerchants(raw.merchants.toDF())

      DataValidation.qualityReport(
        spark,
        raw = Map(
          "transactions" -> raw.transactions.toDF(),
          "users" -> raw.users.toDF(),
          "products" -> raw.products.toDF(),
          "merchants" -> raw.merchants.toDF()
        ),
        results = Map(
          "transactions" -> txResult,
          "users" -> usResult,
          "products" -> prResult,
          "merchants" -> meResult
        ),
        outputPath = outputPath
      )

      val validTransactions = txResult.valid.as[Transaction]
      val validUsers = usResult.valid.as[User]
      val validProducts = prResult.valid.as[Product]
      // Q5.2 : merchants est la petite table (600 lignes) -> broadcast lors de la jointure.
      val validMerchants = SparkOptimizations.broadcastIfEnabled(meResult.valid).as[Merchant]

      // ------------------------------------------------------------ 3. Transformation 
      val transformation = new DataTransformation()
      val enriched = transformation.enrichTransactionData(
        validTransactions, validUsers, validProducts, validMerchants)

      // Q5.1 : réutilisé par les deux analyses ci-dessous -> cache.
      val enrichedCached = SparkOptimizations.cacheIfEnabled(enriched)
      val withBehavior = transformation.addBehaviorFeatures(enrichedCached)
      // Volumineux (fenêtres sur toutes les transactions) -> persist disque+mémoire.
      val withBehaviorPersisted = SparkOptimizations.persistLarge(withBehavior)

      // ------------------------------------------------------------ 4. Analytique 
      val analytics = new Analytics()
      val merchantReport = analytics.merchantKpis(withBehaviorPersisted)
      val cohortRetention = analytics.userCohortAnalysis(withBehaviorPersisted)
      val bestCohort = analytics.bestCohortAt3Months(cohortRetention)
      val topProducts = analytics.topProductsByRevenue(withBehaviorPersisted)
      val revenueByCatRegion = analytics.revenueByCategoryAndRegion(withBehaviorPersisted)
      val revenueByPayPeriod = analytics.revenueByPaymentAndPeriod(withBehaviorPersisted)


      println("\n=== KPI PAR MARCHAND (top 20 par chiffre d'affaires) ===")
      merchantReport.orderBy(desc("total_revenue")).show(20, truncate = false)

      println("\n=== MATRICE DE RETENTION PAR COHORTE ===")
      cohortRetention.orderBy("cohort_month", "period_index").show(50, truncate = false)

      println("\n=== MEILLEURE COHORTE A 3 MOIS ===")
      bestCohort.show(false)

      println("\n=== TOP 10 PRODUITS PAR CA (bonus 4.4) ===")
      topProducts.show(false)
      println("\n=== CA PAR CATEGORIE ET REGION (bonus 4.4) ===")
      revenueByCatRegion.show(50, truncate = false)
      println("\n=== CA PAR METHODE DE PAIEMENT ET PERIODE (bonus 4.4) ===")
      revenueByPayPeriod.show(false)

      // ------------------------------------------------------------ 5. Écriture des résultats
      writeCsvAndParquet(withBehaviorPersisted, s"$outputPath/enriched_transactions")
      writeCsvAndParquet(merchantReport, s"$outputPath/merchant_report")
      writeCsvAndParquet(cohortRetention, s"$outputPath/cohort_retention")
      writeCsvAndParquet(bestCohort, s"$outputPath/best_cohort")
      writeCsvAndParquet(topProducts, s"$outputPath/top_products")
      writeCsvAndParquet(revenueByCatRegion, s"$outputPath/revenue_by_category_region")
      writeCsvAndParquet(revenueByPayPeriod, s"$outputPath/revenue_by_payment_period")

      SparkOptimizations.release(enrichedCached)
      SparkOptimizations.release(withBehaviorPersisted)
      ()
    }

    outcome match {
      case Success(_) =>
        println("\nPipeline terminé avec succès.")
        spark.stop()
      case Failure(e) =>
        Console.err.println(s"Échec du pipeline : ${e.getMessage}")
        e.printStackTrace()
        spark.stop()
        sys.exit(1)
    }
  }

  /** Écrit un DataFrame en CSV (1 fichier, avec en-tête) et en Parquet (multi-fichiers).
    * Le format CSV ne supporte pas les colonnes de type tableau (ex: preferred_categories) :
    * on les linéarise en chaîne pour cet export uniquement ; le Parquet garde le type natif. */
  private def writeCsvAndParquet(df: DataFrame, basePath: String): Unit = {
    val csvSafe = df.schema.fields.foldLeft(df) { (acc, field) =>
      field.dataType match {
        case _: ArrayType => acc.withColumn(field.name, concat_ws(",", col(field.name)))
        case _            => acc
      }
    }
    csvSafe.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"${basePath}_csv")
    df.write.mode("overwrite").parquet(s"${basePath}_parquet")
  }
}
