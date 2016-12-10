package kneelnrise.warp10scala.services

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import kneelnrise.warp10scala.model._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Assertion, AsyncFlatSpec, Matchers}

import scala.concurrent.Future

class Warp10QueryClientSpec extends AsyncFlatSpec with Matchers {

  import Warp10CommonTest._

  "Warp10QueryClient" should "create a request with given query" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    Warp10QueryClient.createRequest(RawWarpScript("'READ_TOKEN' 'token' STORE [ $token '~example.drone.*' { 'source' 'example' } NOW -10 ] FETCH")) should ===(HttpRequest(
      method = HttpMethods.POST,
      uri = Warp10CommonTest.warp10Configuration.execUrl,
      entity = "'READ_TOKEN' 'token' STORE [ $token '~example.drone.*' { 'source' 'example' } NOW -10 ] FETCH"
    ))
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

    ScalaFutures.whenReady(
      Source.single(HttpResponse(status = StatusCodes.OK, entity = "TEXT"))
        .via(Warp10QueryClient.transformResponse)
        .toFutureString
    ) {
      result => result should ===("TEXT")
    }
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

    ScalaFutures.whenReady(
      Source.single(HttpResponse(status = StatusCodes.BadRequest, entity = "This is a Bad Request"))
        .via(Warp10QueryClient.transformResponse)
        .toFutureString
        .failed
    ) {
      result => result should ===(Warp10Exception(400, "This is a Bad Request"))
    }
  }

  it should "Transforms the response into GTS entries - 1" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    assertTransformToGTS(
      source =
        """
          |[
          |  [
          |    {
          |      "c": "pkg.tst",
          |      "l": {
          |        "lbl1": "val1"
          |      },
          |      "a": {},
          |      "v": [
          |        [
          |          1449212593312000,
          |          true
          |        ]
          |      ]
          |    },
          |    {
          |      "c": "pkg.tst",
          |      "l": {},
          |      "a": {},
          |      "v": [
          |        [
          |          1449212593312000,
          |          48.39577698614448,
          |          -4.47947203181684,
          |          false
          |        ]
          |      ]
          |    },
          |    {
          |      "c": "pkg.tst",
          |      "l": {
          |        "lbl1": "val1",
          |        "lbl2": "val2"
          |      },
          |      "a": {},
          |      "v": [
          |        [
          |          1449212583312000,
          |          48.39577698614448,
          |          -4.509472036734223,
          |          123,
          |          "value"
          |        ]
          |      ]
          |    },
          |    {
          |      "c": "pkg.tst2",
          |      "l": {
          |        "lbl2": "val2"
          |      },
          |      "a": {},
          |      "v": [
          |        [
          |          1449212593312000,
          |          123,
          |          123.456
          |        ]
          |      ]
          |    }
          |  ]
          |]
        """.stripMargin,
      expected = Seq(
        GTS(Some(1449212593312000l), None, None, "pkg.tst", Map("lbl1" -> "val1"), GTSTrueValue),
        GTS(Some(1449212593312000l), Some(Coordinates(48.39577698614448, -4.47947203181684)), None, "pkg.tst", Map.empty, GTSFalseValue),
        GTS(Some(1449212583312000l), Some(Coordinates(48.39577698614448, -4.509472036734223)), Some(123), "pkg.tst", Map("lbl1" -> "val1", "lbl2" -> "val2"), GTSStringValue("value")),
        GTS(Some(1449212593312000l), None, Some(123), "pkg.tst2", Map("lbl2" -> "val2"), GTSDoubleValue(123.456))
      )
    )
  }

  it should "Transforms the response into GTS entries - 2" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    assertTransformToGTS(
      source =
        """
          |[
          |  [
          |    {
          |      "c": "pkg.tst",
          |      "l": {
          |        "lbl1": "val1"
          |      },
          |      "a": {},
          |      "i": 1,
          |      "v": [
          |        [
          |          1449212593312000,
          |          true
          |        ],
          |        [
          |          1449212593313000,
          |          false
          |        ]
          |      ]
          |    },
          |    {
          |      "i": 1,
          |      "v": [
          |        [
          |          1449212593314000,
          |          true
          |        ]
          |      ]
          |    },
          |    {
          |      "c": "pkg.tst",
          |      "l": {
          |        "lbl1": "val2"
          |      },
          |      "i": 2,
          |      "v": [
          |        [
          |          1449212583312000,
          |          48.39577698614448,
          |          -4.509472036734223,
          |          123,
          |          1
          |        ],
          |        [
          |          1449212583312000,
          |          49.39577698614448,
          |          -5.509472036734223,
          |          124,
          |          2
          |        ],
          |        [
          |          1449212583312000,
          |          47.39577698614448,
          |          -3.509472036734223,
          |          125,
          |          3
          |        ]
          |      ]
          |    },
          |    {
          |      "i": 2,
          |      "v": [
          |        [
          |          1449212583312000,
          |          46.39577698614448,
          |          -2.509472036734223,
          |          126,
          |          4
          |        ]
          |      ]
          |    }
          |  ]
          |]
        """.stripMargin,
      expected = Seq(
        GTS(Some(1449212593312000l), None, None, "pkg.tst", Map("lbl1" -> "val1"), GTSTrueValue),
        GTS(Some(1449212593313000l), None, None, "pkg.tst", Map("lbl1" -> "val1"), GTSFalseValue),
        GTS(Some(1449212593314000l), None, None, "pkg.tst", Map("lbl1" -> "val1"), GTSTrueValue),

        GTS(Some(1449212583312000l), Some(Coordinates(48.39577698614448, -4.509472036734223)), Some(123), "pkg.tst", Map("lbl1" -> "val2"), GTSLongValue(1)),
        GTS(Some(1449212583312000l), Some(Coordinates(49.39577698614448, -5.509472036734223)), Some(124), "pkg.tst", Map("lbl1" -> "val2"), GTSLongValue(2)),
        GTS(Some(1449212583312000l), Some(Coordinates(47.39577698614448, -3.509472036734223)), Some(125), "pkg.tst", Map("lbl1" -> "val2"), GTSLongValue(3)),
        GTS(Some(1449212583312000l), Some(Coordinates(46.39577698614448, -2.509472036734223)), Some(126), "pkg.tst", Map("lbl1" -> "val2"), GTSLongValue(4))
      )
    )
  }

  private def assertTransformToGTS(source: String, expected: Seq[GTS])(implicit warp10ClientContext: Warp10ClientContext): Future[Assertion] = {
    import warp10ClientContext._
    ScalaFutures.whenReady(
      Source.single(source)
        .via(Warp10QueryClient.jsonToGTS)
        .toFutureSeq
    ) {
      result => result should ===(expected)
    }
  }
}
