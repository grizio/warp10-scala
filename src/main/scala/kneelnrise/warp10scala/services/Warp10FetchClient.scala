package kneelnrise.warp10scala.services

import java.util.UUID

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import kneelnrise.warp10scala.model.{FetchQuery, GTS}

import scala.collection.immutable
import scala.util.{Failure, Success}

object Warp10FetchClient {
  def fetch(implicit warp10ClientContext: Warp10ClientContext): Flow[FetchQuery, GTS, NotUsed] = {
    val uuid = UUID.randomUUID().toString
    Flow[FetchQuery]
      .map(createRequest)
      .map(request => request -> uuid)
      .via(warp10ClientContext.poolClientFlow)
      .filter(result => result._2 == uuid) // We ignore results from other requests
      .map(result => result._1)
      .map {
        case Success(httpResponse) => httpResponse
        case Failure(exception) => throw exception
      }
      .via(transformResponse)
      .via(byteStringToGTS)
  }

  private[services] def createRequest(query: FetchQuery)(implicit warp10ClientContext: Warp10ClientContext) =
    HttpRequest(
      method = HttpMethods.GET,
      uri = warp10ClientContext.configuration.fetchUrl + "?" + query.serialize,
      headers = immutable.Seq(`X-Warp10-Token`(warp10ClientContext.configuration.readToken))
    )

  private[services] def transformResponse(implicit warp10ClientContext: Warp10ClientContext): Flow[HttpResponse, ByteString, NotUsed] = {
    import warp10ClientContext._

    Flow[HttpResponse]
      .flatMapConcat { httpResponse =>
        if (httpResponse.status == StatusCodes.OK) {
          httpResponse.entity.dataBytes
        } else {
          Source.fromFuture(
            Warp10CommonClient
              .readAllDataBytes(httpResponse.entity.dataBytes)
              .map(content => Warp10Exception(httpResponse.status.intValue(), content))
              .map(throw _)
          )
        }
      }
  }

  private[services] def byteStringToGTS: Flow[ByteString, GTS, NotUsed] =
    Flow[ByteString]
      .via(Warp10CommonClient.lineByLineNoEmpty)
      .map(GTS.parse)
      // TODO: What should we do when the server returns an invalid entity?
      .filter(_.isRight)
      .map(_.right.get)
}