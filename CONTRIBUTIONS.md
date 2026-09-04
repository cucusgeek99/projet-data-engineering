# CONTRIBUTIONS

## 1. Question -> responsable -> relecteur

| Question | Responsable | Relecteur | Statut |
|----------|-------------|-----------|--------|
| Partie 1 (structure, build.sbt, README) | A | C | Arborescence en place |
| Partie 2 (ingestion, validation, rapport qualité) | A | B | Fait |
| Partie 3 (UDF, enrichissement, fenêtrage) | B | A | À faire |
| Partie 4 (KPI marchands, cohortes) | C | B | Fait par B en l'absence de contribution de C |
| Parties 5 & 6 (optimisations, MainApp) | C | A, B | SparkOptimizations fait par B (absence de C) ; MainApp (Q6.1) écrit par A pour débloquer l'intégration |
| Partie 7 (application.conf) | A | C | Fait |

## 2. Charge de travail et difficultés (par membre)

- **Membre A**  :15h . Difficultés : Travailler avec une pipeline et une architecture bien definie .
- **Membre B** : 12h. Difficultés : Première fois que j'écrivais vraiment du code Scala(j'arrivais difficlement àsuivre tous les ocurs et surtout à pratiquer), il a fallu du temps pour comprendre le fonctionnement des UDF et des window functions . Je me suis appuyé sur l'aide de l'IA pour accélérer l'apprentissage aussi...
- **Membre C** : 0h à ce jour (04/09/2026). Difficultés : aucune contribution reçue ;
  Parties 4, 5 et 6 réalisées par B et A pour permettre la livraison du tronc commun.

## 3. Décisions techniques du groupe (min. 5, à justifier)

1. **Spark 3.5.3 + Scala 2.12.18.**
   Couple le plus stable et le mieux documenté pour Spark : Scala 2.12 reste la version
   de référence de l'écosystème (Scala 2.13/3.x n'apportent rien ici, et `sbt-assembly`
   ainsi que les connecteurs sont mieux éprouvés en 2.12). Compatible avec sbt 1.10.7.

2. **JAR généré via `sbt-assembly`, avec Spark en portée `Provided`.**
   Le jar d'assembly reste léger : Spark est fourni par `spark-submit` au déploiement,
   seuls `Typesafe Config` et le code du projet sont embarqués. `Compile / run` /
   `Compile / runMain` réinjectent les dépendances `Provided` pour que `sbt run`
   fonctionne en local sans jar.

3. **Jointures `left` sur les 4 tables (transactions comme table pivot), `merchants` broadcastée.**
   `transactions` (~138 000 lignes) reste la table pivot ; `users`, `products` et
   `merchants` sont jointes en `left` pour ne perdre aucune transaction, y compris
   celles référençant un utilisateur/produit/marchand invalide ou orphelin (constaté
   en pratique : le marchand `M00389`, rejeté en validation pour commission invalide,
   garde son chiffre d'affaires dans le rapport marchand, avec ses colonnes
   descriptives à `null`). `merchants` (600 lignes, largement plus petite que
   `transactions`) est en plus broadcastée (Q5.2) pour éviter un shuffle inutile.

4. **Format de sortie CSV + Parquet.**
   CSV pour la lisibilité métier immédiate (ouverture tableur, relecture humaine),
   Parquet pour la réexploitation par un autre job Spark. Exception technique : les
   colonnes de type tableau (`preferred_categories`) ne sont pas supportées par le
   writer CSV de Spark — elles sont linéarisées en chaîne (`concat_ws`) pour cet
   export uniquement ; le Parquet conserve le type natif `ARRAY<STRING>`.

5. **Exécution sur Java 17/21 obligatoire, flags `--add-opens`, pas de Java 24+.**
   Spark 3.5.3 embarque Hadoop 3.3.4, qui appelle `Subject.getSubject()` — méthode qui
   lève systématiquement une exception depuis Java 24 (suppression du SecurityManager,
   JEP 486). Constaté en cours de projet (Java 26 → échec immédiat de la SparkSession).
   Java 17 ou 21 est donc requis pour tous les membres, avec les flags `--add-opens`
   déclarés une fois pour toutes dans `build.sbt` (`run`/`test` forkés).

## 4. Journal de relecture croisée

| Date | Module | Auteur | Relecteur | Remarques |
|------|--------|--------|-----------|-----------|
| 04/09/2026 | Partie 2 (models/User.scala) | A | B | Virgule finale en trop au niveau du case class retirée |
| 04/09/2026 | Partie 3 (TimeFeatures.scala, DataTransformation.scala) | B | A | Logique conforme, testée (10 tests + run complet). Note : day_of_week/month en anglais (choix assumé) ; la Q2.2 ne valide que la longueur du timestamp, pas sa validité calendaire (limite héritée du sujet, pas un bug) |
| 04/09/2026 | Parties 4/5 (Analytics.scala, SparkOptimizations.scala) | B (pour C, absent) | A | Bug réel trouvé et quantifié : 3/843 marchands ont NULL au lieu de 0.0 dans les colonnes de tranche d'âge (merchantKpis) quand toutes leurs transactions ont un age_bracket null — correctif à une ligne identifié  |

