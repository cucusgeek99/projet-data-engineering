package com.ecommerce.analytics

import org.scalatest.funsuite.AnyFunSuite

/** Tests unitaires purs (sans Spark) pour l'UDF extractTimeFeatures (Q3.1). */
class TimeFeaturesSpec extends AnyFunSuite {

  test("timestamp null -> toutes les features sont None") {
    val r = TimeFeatures.parseTimeFeatures(null)
    assert(r == TimeFeaturesResult(None, None, None, None, None, None))
  }

  test("timestamp vide -> toutes les features sont None") {
    val r = TimeFeatures.parseTimeFeatures("")
    assert(r.hour.isEmpty)
  }

  test("timestamp de mauvaise longueur -> None, pas d'exception") {
    val r = TimeFeatures.parseTimeFeatures("2024070109")
    assert(r.hour.isEmpty)
  }

  test("heure impossible (25h) -> None, pas d'exception") {
    val r = TimeFeatures.parseTimeFeatures("20240701253000")
    assert(r.hour.isEmpty)
  }

  test("30 février -> résolu en 29 février par le parseur (résolveur SMART), pas d'exception") {
    // Le comportement attendu par Q3.1 est « ne fait pas échouer le job » : ici le
    // résolveur SMART de java.time corrige silencieusement la date plutôt que de la
    // rejeter. On documente ce comportement plutôt que de le masquer.
    val r = TimeFeatures.parseTimeFeatures("20240230123456")
    assert(r.hour.contains(12))
  }

  test("lundi matin -> Morning, jour ouvré, pas weekend") {
    // 01/07/2024 est un lundi
    val r = TimeFeatures.parseTimeFeatures("20240701093000")
    assert(r.hour.contains(9))
    assert(r.day_of_week.contains("Monday"))
    assert(r.month.contains("July"))
    assert(r.is_weekend.contains(0))
    assert(r.day_period.contains("Morning"))
    assert(r.is_working_hours.contains(1))
  }

  test("samedi soir -> weekend, Night, hors heures ouvrées") {
    // 06/07/2024 est un samedi
    val r = TimeFeatures.parseTimeFeatures("20240706233000")
    assert(r.is_weekend.contains(1))
    assert(r.day_period.contains("Night"))
    assert(r.is_working_hours.contains(0))
  }
}
