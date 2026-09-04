package com.ecommerce.analytics

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, TextStyle}
import java.util.Locale
import scala.util.Try

// Question 3.1 : UDF extractTimeFeatures 

case class TimeFeaturesResult(
    hour: Option[Int],
    day_of_week: Option[String],
    month: Option[String],
    is_weekend: Option[Int],
    day_period: Option[String],
    is_working_hours: Option[Int]
)

object TimeFeatures {

  // Format attendu yyyyMMddHHmmss
  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  private val emptyResult =
    TimeFeaturesResult(None, None, None, None, None, None)

  // Logique sans Spark
  def parseTimeFeatures(timestamp: String): TimeFeaturesResult = {
    if (timestamp == null || timestamp.length != 14) {
      emptyResult
    } else {
      Try(LocalDateTime.parse(timestamp, formatter)).toOption match {
        case None =>
          emptyResult

        case Some(dateTime) =>
          val hour = dateTime.getHour

          val dayOfWeek =
            dateTime.getDayOfWeek.getDisplayName(TextStyle.FULL, Locale.ENGLISH)
          val month =
            dateTime.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH)

          // 1 = lundi, 7 = dimanche, etvc
          val isWeekend = if (dateTime.getDayOfWeek.getValue >= 6) 1 else 0

          val dayPeriod =
            if (hour >= 6 && hour < 12) "Morning"
            else if (hour >= 12 && hour < 18) "Afternoon"
            else if (hour >= 18 && hour < 22) "Evening"
            else "Night"

          val isWorkingHours = if (hour >= 9 && hour <= 17) 1 else 0

          TimeFeaturesResult(
            hour = Some(hour),
            day_of_week = Some(dayOfWeek),
            month = Some(month),
            is_weekend = Some(isWeekend),
            day_period = Some(dayPeriod),
            is_working_hours = Some(isWorkingHours)
          )
      }
    }
  }

  val extractTimeFeatures: UserDefinedFunction = udf(parseTimeFeatures _)
}