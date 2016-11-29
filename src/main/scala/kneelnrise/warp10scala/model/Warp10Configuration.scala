package kneelnrise.warp10scala.model

case class Warp10Configuration(
  baseUrl: String,
  readToken: String,
  writeToken: String,
  version: Warp10Configuration.ApiVersion
) {
  def baseUrlWithVersion = s"$baseUrl/api/v${version.version}"

  def pushUrl: String = s"$baseUrlWithVersion/update"

  def fetchUrl: String = s"$baseUrlWithVersion/fetch"
}

object Warp10Configuration {
  case class ApiVersion(version: Long)

  object ApiVersion {
    val ZERO = ApiVersion(0)
  }
}