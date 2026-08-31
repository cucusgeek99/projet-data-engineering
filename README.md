# EcommerceAnalytics

Pipeline Spark / Scala d'analyse de données e-commerce — Projet Final Data Engineer.

Stack : Scala 2.12.18 · Apache Spark 3.5.3 · sbt 1.10.7 · Java 17 (21 supporté).

## À compléter (Q1.3 - Membre A)

- [ ] Prérequis (installation Scala, Spark, sbt)
- [ ] Compilation et génération du JAR
- [ ] Exécution locale avec sbt
- [ ] Déploiement `spark-submit`
- [ ] Tableau avant/après optimisations (Q5.3 bonus)

## Arborescence

```
EcommerceAnalytics/
├── build.sbt
├── project/               (build.properties, plugins.sbt)
├── README.md
├── EQUIPE.md
├── CONTRIBUTIONS.md
├── .gitignore
└── src/
    ├── main/scala/com/ecommerce/
    │   ├── analytics/   DataIngestion, DataValidation (A) · DataTransformation, TimeFeatures (B) · Analytics, SparkOptimizations, MainApp (C)
    │   ├── models/      case classes (A)
    │   └── utils/       SparkSessionBuilder, ConfigLoader (A)
    ├── main/resources/  application.conf + data/
    └── test/scala/com/ecommerce/
```
