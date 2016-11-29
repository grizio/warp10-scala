package kneelnrise.warp10scala.services

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import kneelnrise.warp10scala.model.{Coordinates, GTS, GTSStringValue, GTSTrueValue}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Assertion, AsyncFlatSpec, Matchers}
import sun.net.ConnectionResetException

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class Warp10PushClientSpec extends AsyncFlatSpec with Matchers {
  "Warp10PushClient" should "create a request with given GTS - 1" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    assertCreateRequest(
      GTS(
        ts = None,
        coordinates = None,
        elev = None,
        name = "test",
        labels = Map(),
        value = GTSTrueValue
      ),
      "// test{} T"
    )
  }

  it should "create a request with given GTS - 2" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    assertCreateRequest(
      GTS(
        ts = Some(123456789),
        coordinates = Some(Coordinates(123.45, -67.089)),
        elev = Some(987654321),
        name = "test",
        labels = Map("label1" -> "val1", "label2" -> "val2"),
        value = GTSStringValue("My string")
      ),
      "123456789/123.45:-67.089/987654321 test{label1=val1,label2=val2} 'My string'"
    )
  }

  it should "transform the response in terms of resulting HttpResponse - 1" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    Warp10PushClient.transformResponse(HttpResponse(status = StatusCodes.OK)).map(_ => succeed)
  }

  it should "transform the response in terms of resulting HttpResponse - 2" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    ScalaFutures.whenReady(Warp10PushClient.transformResponse(HttpResponse(status = StatusCodes.Forbidden, entity = "Failed authentication")).failed) {
      result => result should ===(Warp10Exception(403, "Failed authentication"))
    }
  }

  it should "transform the response in terms of resulting HttpResponse - 3" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    ScalaFutures.whenReady(Warp10PushClient.transformResponse(HttpResponse(status = StatusCodes.BadRequest, entity = "Input data is invalid")).failed) {
      result => result should ===(Warp10Exception(400, "Input data is invalid"))
    }
  }

  it should "accept the incoming gts, send it to pool client and return the response - 1" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = createContextForPushTest()

    ScalaFutures.whenReady(createPush(GTS(
      ts = None,
      coordinates = None,
      elev = None,
      name = "test",
      labels = Map(),
      value = GTSTrueValue
    ))) {
      result =>
        result should ===(())
    }
  }

  it should "accept the incoming gts, send it to pool client and return the response - 2" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = createContextForPushTest()

    ScalaFutures.whenReady(createPush(GTS(
      ts = Some(123456789),
      coordinates = Some(Coordinates(123.45, -67.089)),
      elev = Some(987654321),
      name = "test",
      labels = Map("label1" -> "val1", "label2" -> "val2"),
      value = GTSStringValue("My string")
    ))) {
      result => result should ===(())
    }
  }

  it should "accept the incoming gts, send it to pool client and return the response - 3" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = createContextForPushTest()

    ScalaFutures.whenReady(createPush(GTS(
      ts = None,
      coordinates = None,
      elev = None,
      name = "BAD_REQUEST",
      labels = Map(),
      value = GTSTrueValue
    )).failed) {
      result => result should ===(Warp10Exception(400, "The request is invalid"))
    }
  }

  it should "accept the incoming gts, send it to pool client and return the response - 4" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = createContextForPushTest()

    ScalaFutures.whenReady(createPush(GTS(
      ts = None,
      coordinates = None,
      elev = None,
      name = "FAIL",
      labels = Map(),
      value = GTSTrueValue
    )).failed) {
      result => result shouldBe a[ConnectionResetException]
    }
  }

  private def assertCreateRequest(gts: GTS, entity: String)(implicit warp10ClientContext: Warp10ClientContext): Future[Assertion] = {
    Warp10PushClient.createRequest(gts) should ===(HttpRequest(
      method = HttpMethods.POST,
      uri = Warp10CommonTest.warp10Configuration.pushUrl,
      headers = immutable.Seq(`X-Warp10-Token`(Warp10CommonTest.warp10Configuration.writeToken)),
      entity = HttpEntity(entity)
    ))
  }

  private def createPush(gts: GTS)(implicit warp10ClientContext: Warp10ClientContext): Future[Unit] = {
    import warp10ClientContext._
    Source
      .single(gts)
      .via(Warp10PushClient.push)
      .runWith(Sink.head)
  }

  private def createContextForPushTest()(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, executionContext: ExecutionContext) =
    Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Flow[(HttpRequest, String)].mapAsync(1) { case (httpRequest, requestKey) =>
        Warp10CommonClient.readAllDataBytes(httpRequest.entity.dataBytes)
          .map {
            case "// test{} T" => Success(HttpResponse(StatusCodes.OK))
            case "123456789/123.45:-67.089/987654321 test{label1=val1,label2=val2} 'My string'" => Success(HttpResponse(StatusCodes.OK))
            case "// BAD_REQUEST{} T" => Success(HttpResponse(StatusCodes.BadRequest, entity = "The request is invalid"))
            case "// FAIL{} T" => Failure(new ConnectionResetException())
            case _ => Success(HttpResponse(StatusCodes.NotImplemented))
          }
          .map(httpResponse => (httpResponse, requestKey))
      },
      actorMaterializer = actorMaterializer
    )
}
