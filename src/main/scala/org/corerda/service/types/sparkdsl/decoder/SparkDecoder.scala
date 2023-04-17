package org.corerda.service.types.sparkdsl.decoder

import io.circe.generic.auto._
import io.circe.Decoder
import org.corerda.entities._
import org.corerda.service.types.sparkdsl.SparkDSLImpl._
import org.corerda.service.types.sparkdsl.SparkReaderImpl._
import org.corerda.service.types.sparkdsl.SparkWriterImpl._

object SparkDecoder {
  private def notDefined(str: String): IllegalArgumentException = new IllegalArgumentException(s"$str is not Defined yet")

  implicit def taskDecoder: Decoder[Node[innerType]] = taskCursor =>
    taskCursor.get[String]("type") match {
      case Right("in_fs_json") =>
        for {
          reader <- taskCursor.get[CustomReader]("config")
        } yield Node(Zero, reader)
      case Right("free") =>
        for {
          reader <- taskCursor.get[FreeReader]("config")
        } yield Node(Zero, reader)
      case Right("transformer") =>
        for {
          from <- taskCursor.get[String]("from")
          transf <- taskCursor.get[TransformerCmp]("config")
        } yield Node(One(from), transf)
      case Right("binder") =>
        for {
          left <- taskCursor.get[String]("left")
          right <- taskCursor.get[String]("right")
          binder <- taskCursor.get[BinderCmp]("config")
        } yield Node(Two(left, right), binder)
      case Right("console") =>
        for {
          from <- taskCursor.get[String]("from")
          writer <- taskCursor.get[ConsoleWriter]("config")
        } yield Node(One(from), writer)
      case Right("out_fs_json") =>
        for {
          from <- taskCursor.get[String]("from")
          writer <- taskCursor.get[CustomWriter]("config")
        } yield Node(One(from), writer)
      case Right(other) => throw notDefined(other)
    }
}
