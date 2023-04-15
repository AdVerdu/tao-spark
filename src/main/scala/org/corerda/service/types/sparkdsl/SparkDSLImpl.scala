package org.corerda.service.types.sparkdsl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, explode_outer, lit}
import org.corerda.entities._

object SparkDSLImpl {
  type innerType = DataFrame

  case class TransformerCmp(transformer: List[String], tag: String) extends Transformer[innerType] {
    val lookup: List[innerType => innerType] =
      transformer.map {
        case s"$source:flat:$target" => (data: DataFrame) => data.withColumn(target, col(source))
        case s"$source:lit_$str:$target" => (data: DataFrame) => data.withColumn(target, concat(col(source), lit(s"_$str")))
        case s"$source:${str}_lit:$target" => (data: DataFrame) => data.withColumn(target, concat(lit(s"${str}_"), col(source)))
        case s"$left:rconcat:$right" => (data: DataFrame) => data.withColumn(left, concat(col(left), lit("-"), col(right)))
        case s"$source:explode_outer:$target" => (data: DataFrame) => data.withColumn(target, explode_outer(col(source)))
        case s"select:$keys" => (data: DataFrame) =>
          val cols = keys.split(",")
          data.select(cols.head, cols.tail: _*)
      }

    def f(data: innerType): innerType = lookup.foldLeft(data)((ca, f) => f(ca))
  }

  case class BinderCmp(mode: String, keys: Seq[String], tag: String) extends Binder[innerType] {
    val operation: (innerType, innerType) => innerType = (left, right) => left.join(right, keys, mode)

    def bind(left: innerType, right: innerType): innerType = operation(left, right)
  }
}
