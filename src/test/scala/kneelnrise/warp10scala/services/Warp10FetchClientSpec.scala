package kneelnrise.warp10scala.services

import java.time.{LocalDateTime, ZoneOffset}

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import kneelnrise.warp10scala.model._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Assertion, AsyncFlatSpec, Matchers}
import sun.net.ConnectionResetException

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class Warp10FetchClientSpec extends AsyncFlatSpec with Matchers {

  import Warp10CommonTest._

  "Warp10FetchClient" should "create a request with given query - 1" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    assertCreateRequest(
      FetchQuery(
        selector = Selector(
          name = "package.tst",
          labels = Map(
            "lbl1" -> "val1",
            "lbl2" -> "val2"
          ),
          asPattern = false
        ),
        interval = FetchInterval(
          start = LocalDateTime.parse("2016-01-01T00:00:00"),
          end = LocalDateTime.parse("2016-12-31T23:59:59")
        ),
        dedup = true
      ),
      "selector=package.tst{lbl1=val1,lbl2=val2}&format=fulltext&dedup=true&start=2016-01-01T00:00&end=2016-12-31T23:59:59"
    )
  }

  it should "create a request with given query - 2" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    val now = LocalDateTime.parse("2016-01-01T00:00:00")

    assertCreateRequest(
      FetchQuery(
        selector = Selector(
          name = "package.*",
          labels = Map(
            "lbl1" -> "*",
            "lbl2" -> "2.*"
          ),
          asPattern = true
        ),
        interval = FetchInterval(
          now = now,
          interval = 2.hours
        ),
        dedup = false
      ),
      s"selector=~package.*{lbl1~*,lbl2~2.*}&format=fulltext&dedup=false&now=${now.toEpochSecond(ZoneOffset.UTC) * 1000000}&timespan=7200000000"
    )
  }

  it should "create a request with given query - 3" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    val now = LocalDateTime.parse("2016-01-01T00:00:00")

    assertCreateRequest(
      FetchQuery(
        selector = Selector(
          name = ExactNameSelector("package.tst"),
          labels = Seq(
            ExactLabelSelector("lbl1", "val1"),
            PatternLabelSelector("lbl2", "2.*")
          )
        ),
        interval = FetchInterval(
          now = now,
          numberOfEntries = 10
        )
      ),
      s"selector=package.tst{lbl1=val1,lbl2~2.*}&format=fulltext&dedup=false&now=${now.toEpochSecond(ZoneOffset.UTC) * 1000000}&timespan=-10"
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

    ScalaFutures.whenReady(
      Source.single(HttpResponse(status = StatusCodes.OK, entity = "TEXT"))
        .via(Warp10FetchClient.transformResponse)
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
        .via(Warp10FetchClient.transformResponse)
        .toFutureString
        .failed
    ) {
      result => result should ===(Warp10Exception(400, "This is a Bad Request"))
    }
  }

  it should "Transforms the response into GTS entries" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )

    assertTransformToGTS(
      parts = Seq(
        "1434590504758000// com.cityzendata.test.sinusoide{steps=100,.app=com.cityzendata.drill.test} -0.6133061918698982\n",
        "1434590288758000// com.cityzendata.test.sinusoide{steps=100,.app=com.cityzendata.drill.test} 0.9228427144511169\r\n",
        "1434590072758000// com.cityzendata.test.sinusoide{steps=100,.app=com.cityzendata.drill.test} 0.1301889411087915\n1434589856758000// com.cityzendata.test.sinusoide{steps=100,.app=com.cityzendata.drill.test} -0.9909979074466949\n",
        "1434589640758000// com.cityzendata.test.sinusoide{steps=100,",
        ".app=com.cityzendata.drill.test} 0.38860822449533416\n1434589424758000// com.cityzendata.test.sinusoide{steps=100,",
        ".app=com.cityzendata.drill.test} 0.7875576742396682",
        "\n1434589208758000// com.cityzendata.test.sinusoide{steps=100,.app=com.cityzendata.drill.test} -0.8009024241854527\n",
        "1434588992758000// com.cityzendata.test.sinusoide{steps=100,.app=com.cityzendata.drill.test} -0.36827736651082893",
        "\n1434588776758000// com.cityzendata.test.sinusoide{steps=100,.app=com.cityzendata.drill.test} 0.993699252984253\n\n",
        "1434588560758000// com.cityzendata.test.sinusoide{steps=100,.app=com.cityzendata.drill.test} -0.15193398008971615"
      ),
      expected = Seq(
        GTS(Some(1434590504758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(-0.6133061918698982)),
        GTS(Some(1434590288758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(0.9228427144511169)),
        GTS(Some(1434590072758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(0.1301889411087915)),
        GTS(Some(1434589856758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(-0.9909979074466949)),
        GTS(Some(1434589640758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(0.38860822449533416)),
        GTS(Some(1434589424758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(0.7875576742396682)),
        GTS(Some(1434589208758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(-0.8009024241854527)),
        GTS(Some(1434588992758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(-0.36827736651082893)),
        GTS(Some(1434588776758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(0.993699252984253)),
        GTS(Some(1434588560758000l), None, None, "com.cityzendata.test.sinusoide", Map("steps" -> "100", ".app" -> "com.cityzendata.drill.test"), GTSDoubleValue(-0.15193398008971615))
      )
    )
  }

  it should "Accept the incoming query and returns the response - 1" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = createContextForFetchTest()

    ScalaFutures.whenReady(createFetch(FetchQuery(Selector("tst1{}"), FetchInterval(start = LocalDateTime.now(), end = LocalDateTime.now())))) {
      result =>
        result should ===(Seq(
          GTS(Some(1434590504758000l), None, None, "tst1", Map("steps" -> "100"), GTSDoubleValue(-0.6133061918698982)),
          GTS(Some(1434590288758000l), None, None, "tst1", Map("steps" -> "100"), GTSDoubleValue(0.9228427144511169)),
          GTS(Some(1434590072758000l), None, None, "tst1", Map("steps" -> "100"), GTSDoubleValue(0.1301889411087915))
        ))
    }
  }

  it should "Accept the incoming query and returns the response - 2" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = createContextForFetchTest()

    ScalaFutures.whenReady(createFetch(FetchQuery(Selector("tst2{}"), FetchInterval(start = LocalDateTime.now(), end = LocalDateTime.now())))) {
      result =>
        result should ===(Seq(
          GTS(Some(1434589856758000l), None, None, "tst2", Map("steps" -> "100"), GTSDoubleValue(-0.9909979074466949)),
          GTS(Some(1434589640758000l), None, None, "tst2", Map("steps" -> "100"), GTSDoubleValue(0.38860822449533416)),
          GTS(Some(1434589424758000l), None, None, "tst2", Map("steps" -> "100"), GTSDoubleValue(0.7875576742396682))
        ))
    }
  }

  it should "Accept the incoming query and returns the response - 3" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val context = createContextForFetchTest()

    ScalaFutures.whenReady(createFetch(FetchQuery(Selector("fail{}"), FetchInterval(start = LocalDateTime.now(), end = LocalDateTime.now()))).failed) {
      result => result shouldBe a[ConnectionResetException]
    }
  }

  private def assertCreateRequest(fetchQuery: FetchQuery, httpQuery: String)(implicit warp10ClientContext: Warp10ClientContext): Future[Assertion] = {
    Warp10FetchClient.createRequest(fetchQuery) should ===(HttpRequest(
      method = HttpMethods.GET,
      uri = Warp10CommonTest.warp10Configuration.fetchUrl + "?" + httpQuery,
      headers = immutable.Seq(`X-Warp10-Token`(Warp10CommonTest.warp10Configuration.readToken))
    ))
  }

  private def assertTransformToGTS(parts: Seq[String], expected: Seq[GTS])(implicit warp10ClientContext: Warp10ClientContext): Future[Assertion] = {
    import warp10ClientContext._
    ScalaFutures.whenReady(
      Source(parts.to[immutable.Iterable])
        .map(x => ByteString(x))
        .via(Warp10FetchClient.byteStringToGTS)
        .toFutureSeq
    ) {
      result => result should ===(expected)
    }
  }

  private def createContextForFetchTest()(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, executionContext: ExecutionContext) =
    Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Flow[(HttpRequest, String)].map { case (httpRequest, requestKey) =>
        (httpRequest.uri.rawQueryString match {
          case Some(query) if query.contains("selector=tst1") => Success(HttpResponse(StatusCodes.OK, entity =
            """
              |1434590504758000// tst1{steps=100} -0.6133061918698982
              |1434590288758000// tst1{steps=100} 0.9228427144511169
              |1434590072758000// tst1{steps=100} 0.1301889411087915
            """.stripMargin))
          case Some(query) if query.contains("selector=tst2") => Success(HttpResponse(StatusCodes.OK, entity =
            """
              |1434589856758000// tst2{steps=100} -0.9909979074466949
              |1434589640758000// tst2{steps=100} 0.38860822449533416
              |1434589424758000// tst2{steps=100} 0.7875576742396682
            """.stripMargin))
          case Some(query) if query.contains("selector=fail") => Failure(new ConnectionResetException())
          case _ => Success(HttpResponse(StatusCodes.BadRequest, entity = "The request is invalid"))
        }, requestKey)
      },
      actorMaterializer = actorMaterializer
    )

  private def createFetch(fetchQuery: FetchQuery)(implicit warp10ClientContext: Warp10ClientContext): Future[Seq[GTS]] = {
    import warp10ClientContext._
    Source.single(fetchQuery)
      .via(Warp10FetchClient.fetch)
      .toFutureSeq
  }
}
