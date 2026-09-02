package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Try

object ConfigLoader {
  lazy val config: Config = ConfigFactory.load()   // lit application.conf du classpath
  def getString(p: String, d: String)   = Try(config.getString(p)).getOrElse(d)
  def getInt(p: String, d: Int)          = Try(config.getInt(p)).getOrElse(d)
  def getDouble(p: String, d: Double)    = Try(config.getDouble(p)).getOrElse(d)
  def getBoolean(p: String, d: Boolean)  = Try(config.getBoolean(p)).getOrElse(d)
}
