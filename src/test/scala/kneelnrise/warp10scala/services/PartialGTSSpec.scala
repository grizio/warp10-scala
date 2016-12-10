package kneelnrise.warp10scala.services

import kneelnrise.warp10scala.model._
import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString}

import scala.collection.immutable

class PartialGTSSpec extends FlatSpec with Matchers {
  "PartialGTS" should "transform a valid JsValue into PartialGTS - 1" in {
    val source = JsObject(
      "c" -> JsString("pkg.tst"),
      "l" -> JsObject(
        "label1" -> JsString("value1"),
        "label2" -> JsString("value2"),
        "label3" -> JsString("value3"),
        "label4" -> JsString("value4")
      ),
      "a" -> JsObject(
        "attr1" -> JsString("val1"),
        "attr2" -> JsString("val2")
      ),
      "i" -> JsNumber(1),
      "v" -> JsArray(JsArray(
        JsNumber(1449212593312000l),
        JsNumber(48.35577699355781),
        JsNumber(-4.47947203181684),
        JsNumber(125l),
        JsBoolean(true)
      ))
    )
    val expected = PartialGTS(
      name = Some("pkg.tst"),
      labels = Some(Map(
        "label1" -> "value1",
        "label2" -> "value2",
        "label3" -> "value3",
        "label4" -> "value4"
      )),
      attributes = Some(Map(
        "attr1" -> "val1",
        "attr2" -> "val2"
      )),
      id = Some(1),
      values = immutable.Iterable(PartialGTSValue(
        ts = 1449212593312000l,
        coordinates = Some(Coordinates(48.35577699355781, -4.47947203181684)),
        elev = Some(125l),
        value = GTSTrueValue
      ))
    )

    PartialGTS.partialGtsReader.read(source) should ===(expected)
  }

  it should "transform a valid JsValue into PartialGTS - 2" in {
    val source = JsObject(
      "i" -> JsNumber(1),
      "v" -> JsArray(JsArray(
        JsNumber(1449212593312000l),
        JsNumber(123)
      ))
    )
    val expected = PartialGTS(
      name = None,
      labels = None,
      attributes = None,
      id = Some(1),
      values = immutable.Iterable(PartialGTSValue(
        ts = 1449212593312000l,
        coordinates = None,
        elev = None,
        value = GTSLongValue(123)
      ))
    )

    PartialGTS.partialGtsReader.read(source) should ===(expected)
  }

  it should "transform a valid JsValue into PartialGTS - 3" in {
    val source = JsObject(
      "c" -> JsString("pkg.tst"),
      "l" -> JsObject("label1" -> JsString("value1")),
      "a" -> JsObject(),
      "v" -> JsArray(JsArray(
        JsNumber(1449212593312000l),
        JsNumber(123),
        JsNumber(123.456)
      ))
    )
    val expected = PartialGTS(
      name = Some("pkg.tst"),
      labels = Some(Map("label1" -> "value1")),
      attributes = Some(Map.empty),
      id = None,
      values = immutable.Iterable(PartialGTSValue(
        ts = 1449212593312000l,
        coordinates = None,
        elev = Some(123),
        value = GTSDoubleValue(123.456)
      ))
    )

    PartialGTS.partialGtsReader.read(source) should ===(expected)
  }

  it should "transform a valid JsValue into PartialGTS - 4" in {
    val source = JsObject(
      "c" -> JsString("pkg.tst"),
      "l" -> JsObject("label1" -> JsString("value1")),
      "a" -> JsObject(),
      "v" -> JsArray(JsArray(
        JsNumber(1449212593312000l),
        JsNumber(123.456),
        JsNumber(456.789),
        JsString("value")
      ))
    )
    val expected = PartialGTS(
      name = Some("pkg.tst"),
      labels = Some(Map("label1" -> "value1")),
      attributes = Some(Map.empty),
      id = None,
      values = immutable.Iterable(PartialGTSValue(
        ts = 1449212593312000l,
        coordinates = Some(Coordinates(123.456, 456.789)),
        elev = None,
        value = GTSStringValue("value")
      ))
    )

    PartialGTS.partialGtsReader.read(source) should ===(expected)
  }

  it should "transform a valid JsValue into PartialGTS - 5" in {
    val source = JsObject(
      "c" -> JsString("pkg.tst"),
      "l" -> JsObject("label1" -> JsString("value1")),
      "a" -> JsObject(),
      "v" -> JsArray(
        JsArray(
          JsNumber(1449212593312000l),
          JsString("value")
        ),
        JsArray(
          JsNumber(1449212593312000l),
          JsNumber(123.456),
          JsNumber(456.789),
          JsBoolean(true)
        ),
        JsArray(
          JsNumber(1449212593312000l),
          JsNumber(123),
          JsNumber(123.456)
        ),
        JsArray(
          JsNumber(1449212593312000l),
          JsNumber(123.456),
          JsNumber(456.789),
          JsNumber(123),
          JsNumber(789)
        )
      )
    )
    val expected = PartialGTS(
      name = Some("pkg.tst"),
      labels = Some(Map("label1" -> "value1")),
      attributes = Some(Map.empty),
      id = None,
      values = immutable.Iterable(
        PartialGTSValue(
          ts = 1449212593312000l,
          coordinates = None,
          elev = None,
          value = GTSStringValue("value")
        ),
        PartialGTSValue(
          ts = 1449212593312000l,
          coordinates = Some(Coordinates(123.456, 456.789)),
          elev = None,
          value = GTSTrueValue
        ),
        PartialGTSValue(
          ts = 1449212593312000l,
          coordinates = None,
          elev = Some(123),
          value = GTSDoubleValue(123.456)
        ),
        PartialGTSValue(
          ts = 1449212593312000l,
          coordinates = Some(Coordinates(123.456, 456.789)),
          elev = Some(123),
          value = GTSLongValue(789)
        )
      )
    )

    PartialGTS.partialGtsReader.read(source) should ===(expected)
  }
}
