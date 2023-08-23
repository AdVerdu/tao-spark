package org.corerda.service.spark.dsl

import org.apache.spark.sql.SaveMode
import org.corerda.entities._
import SparkDSLImpl.innerType


object SparkWriterImpl {

  case class ConsoleWriter(tag: String) extends Writer[innerType] {
    def write(data: innerType): innerType = {
      data.show
      data
    }
  }

  case class CustomWriter(path:String, tag: String) extends Writer[innerType] {
    def write(data: innerType): innerType = {
      data.write
        .format("json")
        .mode(SaveMode.Overwrite)
        .save(path)
      data
    }
  }
}
