package kneelnrise.warp10scala.services

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import kneelnrise.warp10scala.constants.CharsetConstants
import kneelnrise.warp10scala.model.Warp10Configuration
import kneelnrise.warp10scala.services.Warp10CommonClient.PoolClientFlow

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object Warp10CommonClient {
  type PoolClientFlow = Flow[(HttpRequest, String), (Try[HttpResponse], String), _]

  def readAllDataBytes(dataBytesSource: Source[ByteString, _])(implicit actorMaterializer: ActorMaterializer): Future[String] = {
    implicit val executionContext = actorMaterializer.system.dispatcher
    dataBytesSource
      .runFold(ByteString.empty) { case (acc, dataBytes) => acc ++ dataBytes }
      .map(_.decodeString(CharsetConstants.`UTF-8`))
  }

  def lineByLine: Flow[ByteString, String, NotUsed] =
    Flow[ByteString]
      .map(_.decodeString(CharsetConstants.`UTF-8`))
      // Force a last EOL when there is no EOL when EOF
      // TODO: How to delete the last EOL when already defined?
      .intersperse("", "", "\n")
      .scan("") { case (acc, current) =>
        if (acc.endsWith("\n")) {
          current
        } else if (acc.contains("\n")) {
          acc.substring(acc.lastIndexOf("\n") + 1) + current
        } else {
          acc + current
        }
      }
      .filter(_.contains("\n"))
      .map(segment => segment.substring(0, segment.lastIndexOf("\n")))
      .mapConcat(_.split("\n").to[immutable.Iterable])
      .map(line => if (line.startsWith("\r")) line.drop(1) else line)
      .map(line => if (line.endsWith("\r")) line.dropRight(1) else line)

  def lineByLineNoEmpty: Flow[ByteString, String, NotUsed] =
    lineByLine.filter(_.trim.nonEmpty)
}

private[services] case class Warp10ClientContext(configuration: Warp10Configuration, poolClientFlow: Warp10CommonClient.PoolClientFlow, actorMaterializer: ActorMaterializer) {
  implicit def implicitWarp10Configuration: Warp10Configuration = configuration

  implicit def implicitPoolClientFlow: PoolClientFlow = poolClientFlow

  implicit def implicitActorMaterializer: ActorMaterializer = actorMaterializer

  implicit def implicitActorSystem: ActorSystem = actorMaterializer.system

  implicit def implicitExecutionContext: ExecutionContext = actorMaterializer.system.dispatcher
}

case class Warp10Exception(statusCode: Long, error: String) extends Exception(error)

private[services] object `X-Warp10-Token` {
  def apply(value: String): HttpHeader = HttpHeader.parse("X-Warp10-Token", value) match {
    case ParsingResult.Ok(httpHeader, _) => httpHeader
    case ParsingResult.Error(error) => throw Warp10Exception(-1, s"${error.summary}: ${error.detail}")
  }
}