package kneelnrise.warp10scala.services

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import kneelnrise.warp10scala.model.{FetchQuery, GTS, Warp10Configuration}

class Warp10Client private(warp10ClientContext: Warp10ClientContext) {
  def push: Flow[GTS, Unit, NotUsed] = Warp10PushClient.push(warp10ClientContext)

  def fetch: Flow[FetchQuery, GTS, NotUsed] = Warp10FetchClient.fetch(warp10ClientContext)
}

object Warp10Client {

  import Warp10CommonClient._

  def apply(host: String)(implicit warp10configuration: Warp10Configuration, actorSystem: ActorSystem, actorMaterializer: ActorMaterializer): Warp10Client = {
    Warp10Client(Http().cachedHostConnectionPool[String](host))
  }

  def apply(host: String, port: Int)(implicit warp10configuration: Warp10Configuration, actorSystem: ActorSystem, actorMaterializer: ActorMaterializer): Warp10Client = {
    Warp10Client(Http().cachedHostConnectionPool[String](host, port))
  }

  def apply(poolClientFlow: PoolClientFlow)(implicit warp10configuration: Warp10Configuration, actorSystem: ActorSystem, actorMaterializer: ActorMaterializer): Warp10Client = {
    new Warp10Client(Warp10ClientContext(warp10configuration, poolClientFlow, actorMaterializer))
  }
}