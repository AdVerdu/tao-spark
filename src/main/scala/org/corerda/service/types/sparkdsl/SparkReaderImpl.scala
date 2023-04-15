package org.corerda.service.types.sparkdsl

import org.corerda.entities._
import org.corerda.service.types.sparkdsl.SparkDSLImpl.innerType

object SparkReaderImpl {
  val spark = org.apache.spark.sql.SparkSession.active

  case class CustomReader(basePath: String, filters: Map[String, String], tag: String) extends Reader[innerType] {
    val filtersFormat = filters.map(f => s"${f._1}=${f._2}").mkString("/")

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
