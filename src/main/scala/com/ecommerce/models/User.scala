package com.ecommerce.models

case class User (
  user_id: String,  
  age: Option[Int],
  annual_income: Option[Double],
  city: Option[String],
  customer_segment: Option[String],
  preferred_categories: Option[Seq[String]],
  registration_date: Option[String]
)