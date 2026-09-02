# EcommerceAnalytics

Pipeline Spark / Scala d'analyse de données e-commerce distribué — Projet Final Data Engineer.

Le pipeline lit quatre sources hétérogènes (CSV, JSON Lines, Parquet), valide et
nettoie les données, enrichit les transactions (jointures + fenêtrage), puis produit
des indicateurs métier (KPI marchands, cohortes de rétention) écrits en CSV et Parquet.

Voir `SUJET.docx` pour l'énoncé, `EQUIPE.md` pour la répartition et `CONTRIBUTIONS.md`
pour le journal de contribution et les décisions techniques.

---

## Stack technique

| Composant | Version | Remarque |
|-----------|---------|----------|
| Scala | 2.12.18 | version cible de référence de Spark |
| Apache Spark | 3.5.3 | `spark-core`, `spark-sql` (portée `provided`) |
| sbt | 1.10.7 | épinglé dans `project/build.properties` |
| Java (JDK) | **17 ou 21** | **pas 22+** — voir la section « Version de Java » |
| Typesafe Config | 1.4.3 | chargement de `application.conf` |
| sbt-assembly | 2.3.0 | génération du JAR |

---

## Prérequis

### 1. Java 17 ou 21 (obligatoire)

Spark 3.5.3 embarque Hadoop 3.3.4, qui appelle `javax.security.auth.Subject.getSubject()` —
méthode qui **lève une exception à partir de Java 24**. Le projet doit donc tourner sur
un JDK 17 ou 21.



### 2. sbt


Scala est téléchargé automatiquement par sbt : aucune installation séparée.

### 3. Spark (facultatif)

Pour l'exécution locale (`sbt run`), Spark est fourni comme dépendance sbt : rien à installer.
Une installation Spark complète n'est nécessaire que pour un déploiement via `spark-submit` :


### 4. Données

Les quatre jeux de données doivent être présents dans `src/main/resources/data/` :

```
src/main/resources/data/
├── transactions.csv          (~138 000 lignes)
├── users.json                (12 000 lignes, JSON Lines)
├── products.parquet/         (répertoire Parquet de 12 fichiers)
└── merchants.csv             (600 lignes)
```

---

## Compilation

```bash
sbt clean compile        # compilation
sbt test                 # tests unitaires
sbt assembly             # JAR : target/scala-2.12/ecommerce-analytics.jar
```

Le JAR d'assembly est **léger** : Spark est en portée `provided` et n'est donc pas
inclus (il est fourni par `spark-submit` au déploiement).

---

## Exécution locale (sbt)

```bash
sbt run
```

Ou en ciblant explicitement la classe principale :
```bash
sbt "runMain com.ecommerce.analytics.MainApp"
```

`sbt run` est configuré dans `build.sbt` pour :
- réinjecter les dépendances `provided` (Spark) au classpath ;
- forker la JVM avec les options `--add-opens` requises par Spark sur Java 17+.

### Configuration

Tous les paramètres (chemins des fichiers, `master`, seuils de validation,
activation des optimisations) sont externalisés dans
`src/main/resources/application.conf`. Aucune valeur n'est codée en dur dans le code Scala,
et toute clé absente retombe sur une valeur par défaut.

Surcharge sans recompiler :
```bash
sbt run -Dconfig.file=./mon-application.conf
```



---

## Déploiement sur cluster (spark-submit)

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master "local[*]" \
  --conf spark.sql.shuffle.partitions=8 \
  target/scala-2.12/ecommerce-analytics.jar
```

Sur un JDK 17 ou 21, ajouter les options JVM du driver :
```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master "local[*]" \
  --driver-java-options "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" \
  target/scala-2.12/ecommerce-analytics.jar
```

---

## Version de Java

| JDK | Compilation | Exécution |
|-----|-------------|-----------|
| 8 / 11 | ⚠️ non testé | ⚠️ non testé |
| **17** | ✅ | ✅ (avec `--add-opens`, gérés par `build.sbt`) |
| **21** | ✅ | ✅ (avec `--add-opens`, gérés par `build.sbt`) |
| 22 / 23 | ✅ | ⚠️ instable |
| 24+ | ✅ | ❌ `UnsupportedOperationException: getSubject` (Hadoop) |

---

## Arborescence

```
EcommerceAnalytics/
├── build.sbt
├── project/
│   ├── build.properties          (sbt 1.10.7)
│   └── plugins.sbt               (sbt-assembly)
├── README.md
├── EQUIPE.md
├── CONTRIBUTIONS.md
├── .gitignore
└── src/
    ├── main/scala/com/ecommerce/
    │   ├── models/               Transaction · User · Product · Merchant        (Membre A)
    │   ├── utils/                ConfigLoader · SparkSessionBuilder             (Membre A)
    │   └── analytics/
    │       ├── DataIngestion.scala        lecture multi-format                 (Membre A)
    │       ├── DataValidation.scala       validation + rapport qualité         (Membre A)
    │       ├── TimeFeatures.scala         UDF extractTimeFeatures              (Membre B)
    │       ├── DataTransformation.scala   jointures + fenêtrage                (Membre B)
    │       ├── Analytics.scala            KPI marchands + cohortes             (Membre C)
    │       ├── SparkOptimizations.scala   cache / broadcast                    (Membre C)
    │       └── MainApp.scala              orchestration du pipeline            (Membre C)
    ├── main/resources/
    │   ├── application.conf
    │   └── data/                 transactions.csv · users.json · products.parquet · merchants.csv
    └── test/scala/com/ecommerce/
```

---

## Répartition du travail

| Membre | Rôle | Parties | Fichiers |
|--------|------|---------|----------|
| A | Data Ingestion & Platform Engineer | 1, 2, 7 | `models/`, `utils/`, `DataIngestion`, `DataValidation`, `application.conf`, `build.sbt`, `README.md` |
| B | Data Transformation Engineer | 3 | `TimeFeatures`, `DataTransformation` |
| C | Analytics & Performance Engineer | 4, 5, 6 | `Analytics`, `SparkOptimizations`, `MainApp` |

Chaque membre ne modifie que ses fichiers (limitation des conflits Git).

---

## Mesure des optimisations (Q5.3 — bonus)

| Étape | Sans optimisation | Avec optimisation | Gain |
|-------|-------------------|-------------------|------|
| Ingestion | _à mesurer_ | _à mesurer_ | _%_ |
| Transformation | _à mesurer_ | _à mesurer_ | _%_ |
| Analytique | _à mesurer_ | _à mesurer_ | _%_ |
| Écriture | _à mesurer_ | _à mesurer_ | _%_ |

Basculer via `app.optimization.enable-cache` / `app.optimization.enable-broadcast`
dans `application.conf`.
