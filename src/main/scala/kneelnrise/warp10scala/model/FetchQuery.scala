package kneelnrise.warp10scala.model

import java.time.{LocalDateTime, ZoneOffset}

import scala.concurrent.duration.Duration

trait Serializable {
  def serialize: String
}

sealed trait FetchInterval extends Serializable

object FetchInterval {
  def apply(start: LocalDateTime, end: LocalDateTime): FetchInterval = StartStopInterval(start, end)

  def apply(now: LocalDateTime, interval: Duration): FetchInterval = IntervalBefore(now, interval)

  def apply(now: LocalDateTime, numberOfEntries: Long): FetchInterval = LastEntriesFrom(now, numberOfEntries)
}

case class StartStopInterval(start: LocalDateTime, end: LocalDateTime) extends FetchInterval {
  override def serialize: String = s"start=${oldestDate.toString}&end=${newestDate.toString}"

  private def oldestDate = if (start.isBefore(end)) start else end

  private def newestDate = if (start.isBefore(end)) end else start
}

case class IntervalBefore(now: LocalDateTime, interval: Duration) extends FetchInterval {
  override def serialize: String = s"now=${now.toEpochSecond(ZoneOffset.UTC) * 1000000}&timespan=${interval.toMicros}"
}

case class LastEntriesFrom(now: LocalDateTime, numberOfEntries: Long) extends FetchInterval {
  override def serialize: String = s"now=${now.toEpochSecond(ZoneOffset.UTC) * 1000000}&timespan=-${numberOfEntries.abs}"
}

sealed trait Selector extends Serializable

object Selector {
  def apply(name: NameSelector, labels: Seq[LabelSelector]): Selector = TypedSelector(name, labels)

  def apply(raw: String): Selector = RawSelector(raw)

  def apply(name: String, labels: Map[String, String], asPattern: Boolean): Selector = {
    TypedSelector(
      name = NameSelector(name, asPattern),
      labels = labels.map { case (key, value) => LabelSelector(key, value, asPattern) } toSeq
    )
  }
}

case class TypedSelector(name: NameSelector, labels: Seq[LabelSelector]) extends Selector {
  override def serialize: String = s"${name.serialize}{${labels.map(_.serialize).mkString(",")}}"
}

case class RawSelector(selector: String) extends Selector {
  override def serialize: String = selector
}

sealed trait NameSelector extends Serializable

object NameSelector {
  def apply(name: String, asPattern: Boolean = false): NameSelector = {
    if (asPattern) {
      PatternNameSelector(name)
    } else {
      ExactNameSelector(name)
    }
  }
}

case class ExactNameSelector(name: String) extends NameSelector {
  override def serialize: String = name
}

case class PatternNameSelector(name: String) extends NameSelector {
  override def serialize: String = s"~$name"
}

sealed trait LabelSelector extends Serializable

object LabelSelector {
  def apply(key: String, value: String, asPattern: Boolean = false): LabelSelector = {
    if (asPattern) {
      PatternLabelSelector(key, value)
    } else {
      ExactLabelSelector(key, value)
    }
  }
}

case class ExactLabelSelector(key: String, value: String) extends LabelSelector {
  override def serialize: String = s"$key=$value"
}

case class PatternLabelSelector(key: String, value: String) extends LabelSelector {
  override def serialize = s"$key~$value"
}

case class FetchQuery(
  selector: Selector,
  interval: FetchInterval,
  dedup: Boolean = false
) {
  def serialize = s"selector=${selector.serialize}&format=fulltext&dedup=$dedup&${interval.serialize}"
}