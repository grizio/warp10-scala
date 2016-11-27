package kneelnrise.warp10scala.model

import org.scalatest.{FlatSpec, Matchers}

class GTSValueSpec extends FlatSpec with Matchers {
  "GTSValue" should "apply and return a valid type" in {
    GTSValue("test") should ===(GTSStringValue("test"))
    GTSValue(123) should ===(GTSLongValue(123))
    GTSValue(-123) should ===(GTSLongValue(-123))
    GTSValue(123.456) should ===(GTSDoubleValue(123.456))
    GTSValue(-123.456) should ===(GTSDoubleValue(-123.456))
    GTSValue(true) should ===(GTSTrueValue)
    GTSValue(false) should ===(GTSFalseValue)
  }

  it should "parse and accept any valid value and return a GTSValue with right type" in {
    GTSValue.parse("'A string'") should ===(Right(GTSStringValue("A string")))
    GTSValue.parse("123") should ===(Right(GTSLongValue(123)))
    GTSValue.parse("+123") should ===(Right(GTSLongValue(123)))
    GTSValue.parse("-123") should ===(Right(GTSLongValue(-123)))
    GTSValue.parse("123.") should ===(Right(GTSDoubleValue(123.0)))
    GTSValue.parse("-123.") should ===(Right(GTSDoubleValue(-123.0)))
    GTSValue.parse("123.456") should ===(Right(GTSDoubleValue(123.456)))
    GTSValue.parse("+123.456") should ===(Right(GTSDoubleValue(123.456)))
    GTSValue.parse("-123.456") should ===(Right(GTSDoubleValue(-123.456)))
    GTSValue.parse("T") should ===(Right(GTSTrueValue))
    GTSValue.parse("F") should ===(Right(GTSFalseValue))
  }

  it should "not parse and refuse any invalid value" in {
    GTSValue.parse("A string without quote") should ===(Left(InvalidGTSValueFormat))
    GTSValue.parse("123+") should ===(Left(InvalidGTSValueFormat))
    GTSValue.parse("123-") should ===(Left(InvalidGTSValueFormat))
    GTSValue.parse("123.456.789") should ===(Left(InvalidGTSValueFormat))
    GTSValue.parse("True") should ===(Left(InvalidGTSValueFormat))
    GTSValue.parse("False") should ===(Left(InvalidGTSValueFormat))
  }

  it should "serialize into valid string object" in {
    GTSValue("test").serialize should ===("'test'")
    GTSValue(123).serialize should ===("123")
    GTSValue(-123).serialize should ===("-123")
    GTSValue(123.456).serialize should ===("123.456")
    GTSValue(-123.456).serialize should ===("-123.456")
    GTSValue(true).serialize should ===("T")
    GTSValue(false).serialize should ===("F")
  }
}
