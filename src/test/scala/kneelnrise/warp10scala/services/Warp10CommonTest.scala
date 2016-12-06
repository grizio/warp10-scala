package kneelnrise.warp10scala.services

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import kneelnrise.warp10scala.constants.CharsetConstants
import kneelnrise.warp10scala.model.{GTS, Warp10Configuration}

import scala.concurrent.Future
import scala.util.Success

object Warp10CommonTest {
  val emptyPoolClientFlow: Warp10CommonClient.PoolClientFlow = Flow[(HttpRequest, String)].map(_ => (Success(HttpResponse()), ""))
  val warp10Configuration: Warp10Configuration = Warp10Configuration(
    baseUrl = "http://localhost:8080",
    readToken = "rtoken",
    writeToken = "wtoken",
    version = Warp10Configuration.ApiVersion.ZERO
  )

  implicit class SourceByteString(source: Source[ByteString, _]) {
    def toFutureString(implicit materializer: Materializer): Future[String] =
      source.runFold("") { case (acc, current) => acc + current.decodeString(CharsetConstants.`UTF-8`) }
  }

  implicit class SourceGTS(source: Source[GTS, _]) {
    def toFutureSeq(implicit materializer: Materializer): Future[Seq[GTS]] =
      source.runFold(Seq[GTS]()) { case (acc, current) => acc :+ current }
  }
}
