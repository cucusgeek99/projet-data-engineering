# CONTRIBUTIONS

## 1. Question → responsable → relecteur

| Question | Responsable | Relecteur | Statut |
|----------|-------------|-----------|--------|
| Partie 1 (structure, build.sbt, README) | A | C | Arborescence en place |
| Partie 2 (ingestion, validation, rapport qualité) | A | B | Fait, une correction apportée à User.sclala |
| Partie 3 (UDF, enrichissement, fenêtrage) | B | A | À faire |
| Partie 4 (KPI marchands, cohortes) | C | B | À faire |
| Parties 5 & 6 (optimisations, MainApp) | C | A, B | À faire |
| Partie 7 (application.conf) | A | C | Squelette en place |

## 2. Charge de travail et difficultés (par membre)

- **Membre A** — heures : TODO. Difficultés : TODO.
- **Membre B** — heures : 9h. Difficultés : Première fois que j'écrivais vraiment du code Scala(j'arrivais difficlement àsuivre tous les ocurs et surtout à pratiquer), il a fallu du temps pour comprendre le fonctionnement des UDF et des window functions . Je me suis appuyé sur l'aide de l'IA pour accélérer l'apprentissage aussi...
- **Membre C** — heures : TODO. Difficultés : TODO.

## 3. Décisions techniques du groupe (min. 5, à justifier)

1. **Spark 3.5.3 + Scala 2.12.18** — TODO justification.
2. **JAR : sbt-assembly, Spark en `Provided`** — TODO justification.
3. **Stratégie de jointure** — TODO (broadcast des petites tables ?).
4. **Format de sortie CSV + Parquet** — TODO justification.
5. **Exécution sur Java 21 via flags `--add-opens`** — TODO justification.

## 4. Journal de relecture croisée

| Date | Module | Auteur | Relecteur | Remarques |
|------|--------|--------|-----------|-----------|
| 02/09/2026 | Partie 2 (models/User.scala) | A | B | Virgule finale en trop au niveau du case class retirée |
| 04/09/2026 | Partie 2 (DataIngestion / DataValidation) | A | B | Q2.2 et Q2.4 étaient ok mais Q.3 manquait (try-catch + comptage) |
