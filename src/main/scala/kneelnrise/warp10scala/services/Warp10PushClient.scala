package kneelnrise.warp10scala.services

import java.util.UUID

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.stream.scaladsl.Flow
import kneelnrise.warp10scala.model.GTS

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

object Warp10PushClient {

  def push(implicit warp10ClientContext: Warp10ClientContext): Flow[GTS, Unit, NotUsed] = {
    val uuid = UUID.randomUUID().toString
    Flow[GTS]
      .map(createRequest)
      .map(request => request -> uuid)
      .via(warp10ClientContext.poolClientFlow)
      .filter(result => result._2 == uuid) // We ignore results from other requests
      .map(result => result._1)
      .mapAsync(1) {
        case Success(httpResponse) => transformResponse(httpResponse)
        case Failure(exception) => Future.failed(throw exception)
      }
  }

  private[services] def createRequest(gts: GTS)(implicit warp10ClientContext: Warp10ClientContext) =
    HttpRequest(
      method = HttpMethods.POST,
      uri = warp10ClientContext.configuration.pushUrl,
      headers = immutable.Seq(`X-Warp10-Token`(warp10ClientContext.configuration.writeToken)),
      entity = HttpEntity(gts.serialize)
    )

  private[services] def transformResponse(httpResponse: HttpResponse)(implicit warp10ClientContext: Warp10ClientContext): Future[Unit] = {
    import warp10ClientContext._

    if (httpResponse.status == StatusCodes.OK) {
      Future.successful(httpResponse.discardEntityBytes())
    } else {
      Warp10CommonClient.readAllDataBytes(httpResponse.entity.dataBytes)
        .map(content => Warp10Exception(httpResponse.status.intValue(), content))
        .map(throw _)
    }
  }
}
