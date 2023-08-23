package org.corerda.service.spark

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._


object PimpedSpark {
  implicit class ExtendedAPI(df: DataFrame) {
    def getMu(end: Column, start: Column): DataFrame =
      df.select(getTime(avg(unix_timestamp(end) - unix_timestamp(start)))
        .as("mean"))

    def getSigma(end: Column, start: Column): DataFrame =
      df.select(getTime(stddev(unix_timestamp(end) - unix_timestamp(start)))
        .as("stddev"))

    def topBy(groups: Column*): DataFrame =
      df.groupBy(groups: _*)
        .count.as("count")
        .orderBy(desc("count"))
  }

  def timediff(end: Column, start: Column): Column =
    getTime(unix_timestamp(end) - unix_timestamp(start))

  private def getTime(timeCol: Column): Column = date_format(to_timestamp(timeCol), "HH:mm:ss")
}