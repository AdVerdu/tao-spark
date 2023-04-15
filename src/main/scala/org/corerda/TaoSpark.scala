package org.corerda

import io.circe.Decoder
import org.corerda.entities.Node
import org.corerda.rules.core.Job._
import org.corerda.rules.core.Mapper._
import org.corerda.rules.core.TreeOps._
import org.corerda.service.provider.FileProvider
import org.apache.spark.sql.SparkSession
import org.corerda.service.types.sparkdsl.SparkDSLImpl
import org.corerda.service.types.sparkdsl.decoder.SparkDecoder


object TaoSpark extends App {
  type myType = SparkDSLImpl.innerType

  val spark = SparkSession.builder
    .appName("SparkExample")
    .config("spark.master", "local")
    .getOrCreate()

  val planCfg = FileProvider.fromPath("src/main/resources/plans/example1.yml")
  implicit val decoder: Decoder[Node[myType]] = SparkDecoder.taskDecoder
  val graph = fromString[myType](planCfg)
  val treeGraph = toTree(graph)

  treeGraph.map(foldTree)
}
