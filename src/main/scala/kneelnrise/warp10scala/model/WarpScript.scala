package kneelnrise.warp10scala.model

sealed trait WarpScript {
  def serialize: String
}

case class RawWarpScript(query: String) extends WarpScript {
  override def serialize: String = query
}