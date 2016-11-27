package kneelnrise.warp10scala.model

import org.scalatest.{FlatSpec, Matchers}


class GTSSpec extends FlatSpec with Matchers {
  "GTS" should "accept and parse any valid string" in {
    GTS.parse("1380475081000000// foo{label0=val0,label1=val1} 123") should ===(Right(GTS(
      ts = Some(1380475081000000l),
      coordinates = None,
      elev = None,
      name = "foo",
      labels = Map(
        "label0" -> "val0",
        "label1" -> "val1"
      ),
      value = GTSLongValue(123)
    )))

    GTS.parse("/48.0:-4.5/ bar{label0=val0} 3.14") should ===(Right(GTS(
      ts = None,
      coordinates = Some(Coordinates(48.0,-4.5)),
      elev = None,
      name = "bar",
      labels = Map(
        "label0" -> "val0"
      ),
      value = GTSDoubleValue(3.14)
    )))

    GTS.parse("1380475081123456/45.0:-0.01/10000000 foobar{label1=val1} T") should ===(Right(GTS(
      ts = Some(1380475081123456l),
      coordinates = Some(Coordinates(45.0,-0.01)),
      elev = Some(10000000l),
      name = "foobar",
      labels = Map(
        "label1" -> "val1"
      ),
      value = GTSBooleanValue(true)
    )))
  }

  it should "refuse any invalid string with an information of the error" in {
    GTS.parse("") should ===(Left(InvalidGTSStructureFormat))
    GTS.parse("//  ''") should ===(Left(InvalidGTSStructureFormat))
    GTS.parse("// {} ''") should ===(Left(ListInvalidGTSFormat(Seq(InvalidGTSNameFormat))))
    GTS.parse("// test with spaces{} ''") should ===(Left(InvalidGTSStructureFormat))
    GTS.parse("// test{keyvalue} ''") should ===(Left(ListInvalidGTSFormat(Seq(InvalidGTSLabelsFormat))))
    GTS.parse("123.456/123.456/123.456 test{keyvalue} X") should ===(Left(InvalidGTSStructureFormat))
    GTS.parse("123.456/123x:456y/123.456 test{keyvalue} X") should ===(Left(ListInvalidGTSFormat(Seq(
      InvalidGTSTimestampFormat,
      InvalidGTSCoordinatesFormat,
      InvalidGTSElevationFormat,
      InvalidGTSLabelsFormat,
      InvalidGTSValueFormat
    ))))
  }

  it should "serialize into valid string line" in {
    GTS(
      ts = None,
      coordinates = None,
      elev = None,
      name = "test",
      labels = Map(),
      value = GTSTrueValue
    ).serialize should ===("// test{} T")

    GTS(
      ts = Some(123),
      coordinates = Some(Coordinates(123.456,-798.123)),
      elev = Some(456),
      name = "test",
      labels = Map(
        "label1" -> "val1",
        "label2" -> "val2",
        "abc" -> "def",
        "xyz" -> "uvw"
      ),
      value = GTSTrueValue
    ).serialize should ===("123/123.456:-798.123/456 test{label1=val1,label2=val2,abc=def,xyz=uvw} T")

    GTS(
      ts = None,
      coordinates = Some(Coordinates(-1.2,987654.321)),
      elev = None,
      name = "test",
      labels = Map(
        "key" -> "value"
      ),
      value = GTSTrueValue
    ).serialize should ===("/-1.2:987654.321/ test{key=value} T")
  }
}
