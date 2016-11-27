package kneelnrise.warp10scala.model

import java.net.URLDecoder

import kneelnrise.warp10scala.constants.CharsetConstants

case class GTS(
  ts: Option[Long],
  coordinates: Option[Coordinates],
  elev: Option[Long],
  name: String,
  labels: Map[String, String],
  value: GTSValue
) {
  def serialize: String = s"$serializeTs/$serializeCoordinates/$serializeElev $serializeName$serializeLabels $serializeValue"

  private def serializeTs = ts.map(_.toString).getOrElse("")

  private def serializeCoordinates = coordinates.map(_.serialize).getOrElse("")

  private def serializeElev = elev.map(_.toString).getOrElse("")

  private def serializeName = name

  private def serializeLabels = labels.map(pair => pair._1 + "=" + pair._2).mkString("{", ",", "}")

  private def serializeValue = value.serialize
}

case class Coordinates(lat: Double, lon: Double) {
  def serialize: String = s"$lat:$lon"
}

sealed trait GTSValue {
  def serialize: String
}

case class GTSLongValue(value: Long) extends GTSValue {
  override def serialize: String = value.toString
}

case class GTSDoubleValue(value: Double) extends GTSValue {
  override def serialize: String = value.toString
}

sealed trait GTSBooleanValue extends GTSValue {
  def value: Boolean
}

case object GTSTrueValue extends GTSBooleanValue {
  override def value: Boolean = true
  override def serialize: String = "T"
}

case object GTSFalseValue extends GTSBooleanValue {
  override def value: Boolean = false
  override def serialize: String = "F"
}

case class GTSStringValue(value: String) extends GTSValue {
  override def serialize: String = s"'$value'"
}

object GTS {
  private val MainStructureRegex = "([^/]*)/([^/]*:[^/]*)?/([^ ]*) ([^ ]*)\\{([^}]*)\\} (.*)".r

  def parse(input: String): Either[InvalidGTSFormat, GTS] = {
    input match {
      case MainStructureRegex(tsAsString, coordinatesAsString, elevAsString, nameAsString, labelsAsString, valueAsString) =>
        val tsEither = parseLong(notNullString(tsAsString), InvalidGTSTimestampFormat)
        val coordinatesEither = parseCoordinates(notNullString(coordinatesAsString))
        val elevEither = parseLong(notNullString(elevAsString), InvalidGTSElevationFormat)
        val nameEither = if (notNullString(nameAsString).nonEmpty) Right(nameAsString) else Left(InvalidGTSNameFormat)
        val labelsEither = parseLabels(notNullString(labelsAsString))
        val valueEither = GTSValue.parse(notNullString(valueAsString))
        (tsEither, coordinatesEither, elevEither, nameEither, labelsEither, valueEither) match {
          case (Right(ts), Right(coordinates), Right(elev), Right(name), Right(labels), Right(value)) =>
            Right(GTS(
              ts = ts,
              coordinates = coordinates,
              elev = elev,
              name = name,
              labels = labels,
              value = value
            ))
          case _ =>
            Left(ListInvalidGTSFormat(
              Seq(tsEither, coordinatesEither, elevEither, nameEither, labelsEither, valueEither)
                .filter(_.isLeft)
                .map(_.left)
                .map(_.get)
            ))
        }
      case _ => Left(InvalidGTSStructureFormat)
    }
  }

  private def notNullString(input: String) = if (input == null) "" else input

  private def parseCoordinates(input: String): Either[InvalidGTSFormat, Option[Coordinates]] = {
    if (input.nonEmpty) {
      val coordinatesParts = input.split(":")
      if (coordinatesParts.length == 2) {
        val latEither = parseDouble(coordinatesParts(0), InvalidGTSCoordinatesFormat)
        val lonEither = parseDouble(coordinatesParts(1), InvalidGTSCoordinatesFormat)
        (latEither, lonEither) match {
          case (Right(Some(lat)), Right(Some(lon))) => Right(Some(Coordinates(lat, lon)))
          case _ => Left(InvalidGTSCoordinatesFormat)
        }
      } else {
        Left(InvalidGTSCoordinatesFormat)
      }
    } else {
      Right(None)
    }
  }

  private def parseLabels(input: String): Either[InvalidGTSFormat, Map[String, String]] = {
    if (input.nonEmpty) {
      val keyAndValueAsStringSeq = input.split(",")
      if (keyAndValueAsStringSeq.forall(_.contains("="))) {
        val encodedKeyAndValuAsPairSeq = keyAndValueAsStringSeq.map(_.split("=")).map(array => array(0) -> array(1))
        val optionalDecodedKeyAndValueAsPairSeq = encodedKeyAndValuAsPairSeq.map {
          case (key, value) =>
            try {
              Some((
                URLDecoder.decode(key, CharsetConstants.`UTF-8`),
                URLDecoder.decode(value, CharsetConstants.`UTF-8`)
              ))
            } catch {
              case _: IllegalArgumentException => None
            }
        }
        if (optionalDecodedKeyAndValueAsPairSeq.contains(None)) {
          Left(InvalidGTSLabelsFormat)
        } else {
          Right(Map(optionalDecodedKeyAndValueAsPairSeq.map(_.get): _*))
        }
      } else {
        Left(InvalidGTSLabelsFormat)
      }
    } else {
      Right(Map())
    }
  }

  private def parseLong = parseCathingException(_.toLong) _

  private def parseDouble = parseCathingException(_.toDouble) _

  private def parseCathingException[A](map: String => A)(input: String, resultOnError: InvalidGTSFormat): Either[InvalidGTSFormat, Option[A]] = {
    if (input.nonEmpty) {
      try {
        Right(Some(map(input)))
      } catch {
        case _: Exception => Left(resultOnError)
      }
    } else {
      Right(None)
    }
  }
}

object GTSValue {
  def apply(value: Long) = GTSLongValue(value)

  def apply(value: Double) = GTSDoubleValue(value)

  def apply(value: Boolean) = GTSBooleanValue(value)

  def apply(value: String) = GTSStringValue(value)

  def parse(string: String): Either[InvalidGTSFormat, GTSValue] = {
    def isStringValue = string.startsWith("'") && string.endsWith("'") && !string.substring(1, string.length - 1).contains("'")

    def isTrueValue = string == "T"

    def isFalseValue = string == "F"

    def isLongValue = string.matches("(\\+|-)?\\d+")

    def isDoubleValue = string.matches("(\\+|-)?\\d+(\\.\\d*)?")

    if (isStringValue) {
      Right(GTSValue(string.substring(1, string.length - 1)))
    } else if (isTrueValue) {
      Right(GTSValue(true))
    } else if (isFalseValue) {
      Right(GTSValue(false))
    } else if (isLongValue) {
      Right(GTSValue(string.toLong))
    } else if (isDoubleValue) {
      Right(GTSValue(string.toDouble))
    } else {
      Left(InvalidGTSValueFormat)
    }
  }
}

object GTSBooleanValue {
  def apply(value: Boolean): GTSBooleanValue = if (value) GTSTrueValue else GTSFalseValue
}

sealed trait InvalidGTSFormat

case object InvalidGTSStructureFormat extends InvalidGTSFormat

case class ListInvalidGTSFormat(errors: Seq[InvalidGTSFormat]) extends InvalidGTSFormat

case object InvalidGTSTimestampFormat extends InvalidGTSFormat

case object InvalidGTSCoordinatesFormat extends InvalidGTSFormat

case object InvalidGTSElevationFormat extends InvalidGTSFormat

case object InvalidGTSNameFormat extends InvalidGTSFormat

case object InvalidGTSLabelsFormat extends InvalidGTSFormat

case object InvalidGTSValueFormat extends InvalidGTSFormat
