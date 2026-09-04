  package com.ecommerce.analytics

  import com.ecommerce.models._
  import org.apache.spark.sql.{Dataset, SparkSession , AnalysisException}
  import org.apache.spark.sql.functions.col
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.DataFrame

  import scala.util.{Failure, Success, Try}

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

    private def safeRead[T](source: String)(read: => Dataset[T]): Dataset[T] =
      Try(read) match {
        case Success(ds) => ds
        case Failure(e: AnalysisException) =>
          println(s"[INGESTION] '$source' : fichier introuvable ou structure incorrecte -> ${e.getMessage}")
          throw e
        case Failure(e) =>
          println(s"[INGESTION] '$source' : ${e.getClass.getSimpleName} -> ${e.getMessage}")
          throw e
      }

    def readTransactions(path: String): Dataset[Transaction] =
      safeRead("transactions.csv") {
        spark.read.option("header","true").schema(txSchema).csv(path).as[Transaction]
      }

    // users.json 
    def readUsers(path: String): Dataset[User] =
      safeRead("users.json") {
        spark.read.json(path)
          .withColumn("age", col("age").cast(IntegerType))
        .as[User]

      } 

    // products.parquet 
    def readProducts(path: String): Dataset[Product] =
      safeRead("products.parquet") {
        spark.read.parquet(path).as[Product]
      }

    // merchants.csv 
    def readMerchants(path: String): Dataset[Merchant] =
      safeRead("merchants.csv") {
        spark.read.option("header", "true").option("inferSchema", "true").csv(path)
          .withColumn("establishment_date", col("establishment_date").cast(StringType))
        .as[Merchant]
    }

  // Pour charger un dataset puis le valider dans un bloc try-catch et renvoie aussi le dataset brut
  def loadAndValidate(
      name: String,
      load: () => DataFrame,
      validate: DataFrame => DataValidation.ValidationResult
    ): (DataFrame, DataValidation.ValidationResult) = {
      try {
        val raw = load()
        val nbLues = raw.count()
        println(s"[$name] $nbLues lignes lues")

        val result = validate(raw)
        val nbValid = result.valid.count()
        println(s"[$name] $nbValid lignes valides après validation")

        (raw, result)
      } catch {
        case e: Exception =>
          println(s"[$name] ERREUR lors du chargement : ${e.getMessage}")
          throw e
      }
  }

    /** Q2.3 : charge les 4 sources et affiche le nombre de lignes LUES (avant validation). */
    def loadAll(paths: Map[String, String]): IngestedData = {
      val data = IngestedData(
        readTransactions(paths("transactions")),
        readUsers(paths("users")),
        readProducts(paths("products")),
        readMerchants(paths("merchants"))
      )
      println("[INGESTION] Lignes lues avant validation :")
      println(f"  transactions = ${data.transactions.count()}%,d")
      println(f"  users        = ${data.users.count()}%,d")
      println(f"  products     = ${data.products.count()}%,d")
      println(f"  merchants    = ${data.merchants.count()}%,d")
      data
    }
  }
  /** Conteneur des 4 datasets bruts, passé à la validation puis à la transformation. */
  case class IngestedData(
    transactions: Dataset[Transaction],
    users: Dataset[User],
    products: Dataset[Product],
    merchants: Dataset[Merchant]
  )

