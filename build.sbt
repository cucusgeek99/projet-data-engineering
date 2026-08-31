ThisBuild / organization := "com.ecommerce"
ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

// Options du compilateur Scala (non fatales : le squelette compile avec des `???`).
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

// Flags requis pour exécuter Spark 3.5.x sur Java 17+ / Java 21.
// Appliqués aux JVM forkées de `run` et `test`. Pour `spark-submit`, voir le README.
lazy val sparkJavaOpts = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
      "com.typesafe"      % "config"     % "1.4.3",
      "org.scalatest"    %% "scalatest"  % "3.2.19" % Test
    ),

    // `sbt run` : réintègre les dépendances `Provided` au classpath et forke
    // la JVM avec les flags Java nécessaires à Spark.
    Compile / run := Defaults
      .runTask(
        Compile / fullClasspath,
        Compile / run / mainClass,
        Compile / run / runner
      )
      .evaluated,
    Compile / runMain := Defaults
      .runMainTask(Compile / fullClasspath, Compile / run / runner)
      .evaluated,
    run / fork := true,
    run / javaOptions ++= sparkJavaOpts,

    // Tests
    Test / fork := true,
    Test / javaOptions ++= sparkJavaOpts,
    Test / parallelExecution := false,

    // JAR exécutable (léger : Spark est `Provided`, fourni par spark-submit).
    assembly / mainClass       := Some("com.ecommerce.analytics.MainApp"),
    assembly / assemblyJarName := "ecommerce-analytics.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
      case "reference.conf" | "application.conf"    => MergeStrategy.concat
      case _                                        => MergeStrategy.first
    }
  )
