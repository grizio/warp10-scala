package kneelnrise.warp10scala.services

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.Flow
import kneelnrise.warp10scala.model.Warp10Configuration

import scala.util.Success

object Warp10CommonTest {
  val emptyPoolClientFlow: Warp10CommonClient.PoolClientFlow = Flow[(HttpRequest, String)].map(_ => (Success(HttpResponse()), ""))
  val warp10Configuration: Warp10Configuration = Warp10Configuration(
    baseUrl = "http://localhost:8080",
    readToken = "rtoken",
    writeToken = "wtoken",
    version = Warp10Configuration.ApiVersion.ZERO
  )
}
