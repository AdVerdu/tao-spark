package org.corerda.service.spark.dsl

import org.corerda.service.spark.Session.spark
import org.corerda.entities._
import SparkDSLImpl.innerType


object SparkReaderImpl {

  case class CustomReader(basePath: String, filters: Map[String, String], tag: String) extends Reader[innerType] {
    private val filtersFormat = filters.map(f => s"${f._1}=${f._2}").mkString("/")

    def read: innerType = spark.read
      .format("json")
      .option("basePath", basePath)
      .load(s"$basePath/$filtersFormat")
  }

  case class FreeReader(format: String, options: Map[String, String], tag: String) extends Reader[innerType] {
    // /!\ god-mode read /!\
    def read: innerType = spark.read
      .format(format)
      .options(options)
      .load()
  }
}
