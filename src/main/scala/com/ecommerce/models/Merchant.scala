package com.ecommerce.models

case class Merchant (
    merchant_id : String,
    name : Option[String],
    category: Option[String],
    region: Option[String],
    commission_rate: Option[Double],
    establishment_date: Option[String]
)