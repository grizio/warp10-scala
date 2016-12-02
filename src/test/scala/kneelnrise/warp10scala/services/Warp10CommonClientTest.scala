package kneelnrise.warp10scala.services

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.collection.immutable

class Warp10CommonClientTest extends AsyncFlatSpec with Matchers {
  "Warp10CommonClient.lineByLine" should "Transforms a response into a source of lines - 1" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    assertTransformLineByLine(
      source = Seq(
        "abc\n",
        "def\n",
        "ghi\n",
        "jkl\n",
        "mno\n",
        "pqr\n",
        "stu\n",
        "vwx\n",
        "yz"
      ),
      expected = Seq(
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yz"
      )
    )
  }

  it should "Transforms a response into a source of lines - 2" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    assertTransformLineByLine(
      source = Seq(
        "abc\r\n",
        "def\r\n",
        "ghi\r\n",
        "jkl\r\n",
        "mno\r\n",
        "pqr\r\n",
        "stu\r\n",
        "vwx\r\n",
        "yz\r\n"
      ),
      expected = Seq(
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yz",
        "" // Forced EOL
      )
    )
  }

  it should "Transforms a response into a source of lines - 3" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    assertTransformLineByLine(
      source = Seq(
        "abc\ndef\n",
        "ghi\njkl\n",
        "mno\npqr\n",
        "stu\nvwx\nyz\n"
      ),
      expected = Seq(
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yz",
        "" // Forced EOL
      )
    )
  }

  it should "Transforms a response into a source of lines - 4" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    assertTransformLineByLine(
      source = Seq(
        "a",
        "b",
        "c",
        "\n",
        "def\n",
        "g",
        "hi",
        "\n",
        "jkl",
        "\nmno\n",
        "pqr\ns",
        "tu\nvwx\nyz"
      ),
      expected = Seq(
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yz"
      )
    )
  }
  "Warp10CommonClient.lineByLineNoEmpty" should "Transforms a response into a source of lines - 1" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    assertTransformLineByLineNoEmpty(
      source = Seq(
        "abc\n",
        "def\n",
        "ghi\n",
        "jkl\n",
        "\n",
        "mno\n",
        "pqr\n",
        "stu\n",
        "\n",
        "vwx\n",
        "yz"
      ),
      expected = Seq(
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yz"
      )
    )
  }

  it should "Transforms a response into a source of lines - 2" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    assertTransformLineByLineNoEmpty(
      source = Seq(
        "abc\r\n",
        "def\r\n",
        "ghi\r\n",
        "jkl\r\n",
        "\r\n",
        "mno\r\n",
        "pqr\r\n",
        "stu\r\n",
        "vwx\r\n",
        "yz\r\n"
      ),
      expected = Seq(
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yz"
      )
    )
  }

  it should "Transforms a response into a source of lines - 3" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    assertTransformLineByLineNoEmpty(
      source = Seq(
        "abc\ndef\n",
        "ghi\njkl\n\n",
        "mno\n\r\npqr\n",
        "stu\nvwx\nyz\n"
      ),
      expected = Seq(
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yz"
      )
    )
  }

  it should "Transforms a response into a source of lines - 4" in {
    implicit val actorSystem = ActorSystem()
    implicit val actorMaterializer = ActorMaterializer()
    implicit val context = Warp10ClientContext(
      configuration = Warp10CommonTest.warp10Configuration,
      poolClientFlow = Warp10CommonTest.emptyPoolClientFlow,
      actorMaterializer = actorMaterializer
    )
    assertTransformLineByLineNoEmpty(
      source = Seq(
        "a",
        "b",
        "c",
        "\n\r\n\r\n",
        "def\n",
        "g",
        "hi",
        "\n",
        "jkl",
        "\n\nmno\n",
        "pqr\r\n\ns",
        "tu\nvwx\nyz"
      ),
      expected = Seq(
        "abc",
        "def",
        "ghi",
        "jkl",
        "mno",
        "pqr",
        "stu",
        "vwx",
        "yz"
      )
    )
  }

  private def assertTransformLineByLine(source: Seq[String], expected: Seq[String])(implicit warp10ClientContext: Warp10ClientContext) = {
    import warp10ClientContext._
    val linesFuture = Source(source.map(ByteString(_)).to[immutable.Iterable])
      .via(Warp10CommonClient.lineByLine)
      .runFold(Seq[String]()) { case (acc, line) => acc :+ line }

    ScalaFutures.whenReady(linesFuture) {
      lines => lines should ===(expected)
    }
  }

  private def assertTransformLineByLineNoEmpty(source: Seq[String], expected: Seq[String])(implicit warp10ClientContext: Warp10ClientContext) = {
    import warp10ClientContext._
    val linesFuture = Source(source.map(ByteString(_)).to[immutable.Iterable])
      .via(Warp10CommonClient.lineByLineNoEmpty)
      .runFold(Seq[String]()) { case (acc, line) => acc :+ line }

    ScalaFutures.whenReady(linesFuture) {
      lines => lines should ===(expected)
    }
  }
}
