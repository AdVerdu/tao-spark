package org.corerda

import io.circe.Decoder
import org.corerda.entities.{InputParam, Node}
import org.corerda.rules.core.Mapper._
import org.corerda.rules.core.TreeOps._
import org.corerda.service.provider.FileProvider
import org.corerda.service.decoder.SparkDecoder
import org.corerda.service.spark.dsl.SparkDSLImpl


object TaoSpark {

  def main(args: Array[String]): Unit = {
    InputParam.input.parse(args, InputParam()) match {
      case Some(config) =>
        runApplication(config)
      case None =>
      // scopt will handle displaying the usage text when parsing fails
    }
  }

  private def runApplication(config: InputParam): Unit = {
    println(s"Program Name: ${config.jobName}")
    println(s"Environment: ${config.environment}")
    // TODO - PM is whole, worth improving with ADT (?)
    //  validation already occurs in Parser but it is not enforced by the type system
    config.jobName match {
      case "example1" =>
        type myType = SparkDSLImpl.innerType

        // TODO - process input params
        val pathResolved = s"src/main/resources/plans/${config.jobName}.yml"

        // load program Tree
        val planCfg = FileProvider.fromPath(pathResolved)
        implicit val decoder: Decoder[Node[myType]] = SparkDecoder.taskDecoder
        val graph = fromString[myType](planCfg)

        // build + run
        runAST(graph)

        print(s"EXIT CODE: 0")
      case "other" => print(s"EXIT CODE: 2")
    }
  }

}
