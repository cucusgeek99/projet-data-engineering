package com.ecommerce.models

case class Transaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  merchant_id: String,
  amount: Option[Double],       // peut être négatif/nul → Option
  timestamp: Option[String],    // peut être mal formé/vide
  location: Option[String],
  payment_method: Option[String],
  category: Option[String]
)