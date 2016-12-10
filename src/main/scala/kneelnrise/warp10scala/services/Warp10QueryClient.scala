package kneelnrise.warp10scala.services

import java.util.UUID

import akka.NotUsed
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.{Flow, Source}
import kneelnrise.warp10scala.model.{Coordinates, GTS, GTSValue, WarpScript}
import spray.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString, JsValue, JsonParser, JsonReader, ParserInput, deserializationError}

import scala.collection.immutable
import scala.collection.immutable.Iterable
import scala.util.{Failure, Success}

object Warp10QueryClient {
  def query(implicit warp10ClientContext: Warp10ClientContext): Flow[WarpScript, GTS, NotUsed] = {
    val uuid = UUID.randomUUID().toString
    Flow[WarpScript]
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
      .via(jsonToGTS)
  }

  private[services] def createRequest(warpScript: WarpScript)(implicit warp10ClientContext: Warp10ClientContext) =
    HttpRequest(
      method = HttpMethods.POST,
      uri = warp10ClientContext.configuration.execUrl,
      entity = warpScript.serialize
    )

  private[services] def transformResponse(implicit warp10ClientContext: Warp10ClientContext): Flow[HttpResponse, String, NotUsed] = {
    import warp10ClientContext._

    Flow[HttpResponse]
      .flatMapConcat { httpResponse =>
        if (httpResponse.status == StatusCodes.OK) {
          Source.fromFuture(Warp10CommonClient.readAllDataBytes(httpResponse.entity.dataBytes))
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

  private[services] def jsonToGTS: Flow[String, GTS, NotUsed] =
    Flow[String]
      .map(str => JsonParser(ParserInput(str)))
      .map(PartialGTS.rootPartialGTSListReader.read(_).to[immutable.Iterable])
      .map(partialGTSIterable => (extractIdInfo(partialGTSIterable), partialGTSIterable))
      .mapConcat { case (idInfo, partialGTSIterable) => partialGTSIterable.map(partialGTS => (idInfo, partialGTS)) }
      .mapConcat { case (idInfo, partialGTS) => partialGTSToGTSList(partialGTS, partialGTS.id.flatMap(id => idInfo.get(id))) }

  private[services] def extractIdInfo(partialGTSIterable: immutable.Iterable[PartialGTS]): immutable.Map[Long, PartialGTS] = {
    var result = Map[Long, PartialGTS]()
    partialGTSIterable
      .filter(_.id.isDefined)
      .foreach{ partialGTS =>
        val entry = result.getOrElse(partialGTS.id.get, partialGTS)
        val newEntry = entry.copy(
          name = entry.name.orElse(partialGTS.name),
          labels = entry.labels.orElse(partialGTS.labels),
          attributes = entry.attributes.orElse(partialGTS.attributes)
        )
        result = result + (partialGTS.id.get -> newEntry)
      }
    result
  }

  private[services] def partialGTSToGTSList(partialGTS: PartialGTS, referencePartialGTSOpt: Option[PartialGTS]): immutable.Iterable[GTS] = {
    referencePartialGTSOpt match {
      case Some(referencePartialGTS) =>
        partialGTS.values.map { partialGTSValue =>
          GTS(
            ts = Some(partialGTSValue.ts),
            coordinates = partialGTSValue.coordinates,
            elev = partialGTSValue.elev,
            name = partialGTS.name.getOrElse(referencePartialGTS.name.getOrElse("")),
            labels = partialGTS.labels.getOrElse(referencePartialGTS.labels.getOrElse(Map.empty)),
            value = partialGTSValue.value
          )
        }
      case None =>
        partialGTS.values.map { partialGTSValue =>
          GTS(
            ts = Some(partialGTSValue.ts),
            coordinates = partialGTSValue.coordinates,
            elev = partialGTSValue.elev,
            name = partialGTS.name.getOrElse(""),
            labels = partialGTS.labels.getOrElse(Map.empty),
            value = partialGTSValue.value
          )
        }
    }
  }
}

private case class PartialGTS(
  name: Option[String],
  labels: Option[Map[String, String]],
  attributes: Option[Map[String, String]],
  id: Option[Long],
  values: immutable.Iterable[PartialGTSValue]
)

private case class PartialGTSValue(
  ts: Long,
  coordinates: Option[Coordinates],
  elev: Option[Long],
  value: GTSValue
)

private object PartialGTS {
  def mapReader(field: String): JsonReader[Map[String, String]] = new JsonReader[Map[String, String]] {
    override def read(json: JsValue): Map[String, String] = {
      json match {
        case JsObject(pairs) =>
          pairs map {
            case (key, JsString(value)) => key -> value
            case _ => deserializationError(s"$field map expected")
          }
        case _ => deserializationError(s"$field map expected")
      }
    }
  }

  val labelsReader: JsonReader[Map[String, String]] = mapReader("labels")

  val attributesReader: JsonReader[Map[String, String]] = mapReader("attributes")

  val valueReader: JsonReader[GTSValue] = new JsonReader[GTSValue] {
    override def read(json: JsValue): GTSValue = {
      json match {
        case JsString(v) => GTSValue(v)
        case JsBoolean(v) => GTSValue(v)
        case JsNumber(v) if v.isValidLong => GTSValue(v.toLong)
        case JsNumber(v) => GTSValue(v.toDouble)
        case _ => deserializationError("value is invalid")
      }
    }
  }

  val partialGTSValueReader: JsonReader[PartialGTSValue] = new JsonReader[PartialGTSValue] {
    override def read(json: JsValue): PartialGTSValue = {
      json match {
        case JsArray(v) =>
          v match {
            case Vector(JsNumber(ts), jsValue) =>
              PartialGTSValue(ts.toLong, None, None, valueReader.read(jsValue))
            case Vector(JsNumber(ts), JsNumber(elev), jsValue) =>
              PartialGTSValue(ts.toLong, None, Some(elev.toLong), valueReader.read(jsValue))
            case Vector(JsNumber(ts), JsNumber(lat), JsNumber(lon), jsValue) =>
              PartialGTSValue(ts.toLong, Some(Coordinates(lat.toDouble, lon.toDouble)), None, valueReader.read(jsValue))
            case Vector(JsNumber(ts), JsNumber(lat), JsNumber(lon), JsNumber(elev), jsValue) =>
              PartialGTSValue(ts.toLong, Some(Coordinates(lat.toDouble, lon.toDouble)), Some(elev.toLong), valueReader.read(jsValue))
          }
        case _ => deserializationError("value information is invalid")
      }
    }
  }

  val partialGTSValueListReader: JsonReader[immutable.Iterable[PartialGTSValue]] = new JsonReader[Iterable[PartialGTSValue]] {
    override def read(json: JsValue): immutable.Iterable[PartialGTSValue] = {
      json match {
        case JsArray(elements) => elements.map(partialGTSValueReader.read)
        case _ => deserializationError("expected list of GTS values")
      }
    }
  }

  val partialGtsReader: JsonReader[PartialGTS] = new JsonReader[PartialGTS] {
    override def read(json: JsValue): PartialGTS = {
      json match {
        case JsObject(fields) =>
          (
            fields.get("c"),
            fields.get("l"),
            fields.get("a"),
            fields.get("i"),
            fields.get("v")
          ) match {
            case (c: Option[JsString], l: Option[JsObject], a: Option[JsObject], id: Option[JsNumber], Some(v: JsArray)) =>
              PartialGTS(
                name = c.map(_.value),
                labels = l.map(labelsReader.read),
                attributes = a.map(attributesReader.read),
                id = id.map(_.value.toLong),
                values = partialGTSValueListReader.read(v)
              )
            case _ => deserializationError("GTS expected")
          }
        case _ => deserializationError("GTS expected")
      }
    }
  }

  val partialGTSListReader: JsonReader[Iterable[PartialGTS]] = new JsonReader[Iterable[PartialGTS]] {
    override def read(json: JsValue): Iterable[PartialGTS] = {
      json match {
        case JsArray(elements) => elements.map(partialGtsReader.read)
        case _ => deserializationError("List of GTS expected")
      }
    }
  }

  val rootPartialGTSListReader: JsonReader[Iterable[PartialGTS]] = new JsonReader[Iterable[PartialGTS]] {
    override def read(json: JsValue): Iterable[PartialGTS] = {
      json match {
        case JsArray(elements) if elements.length == 1 => partialGTSListReader.read(elements.head)
        case _ => deserializationError("List of GTS expected")
      }
    }
  }
}