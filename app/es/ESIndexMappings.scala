package es

import play.api.libs.json.{Format, Json}
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.Future

case class IndexSettings(number_of_shards: Int, number_of_replicas: Int)

object IndexSettings {
  implicit val _: Format[IndexSettings] = Json.format[IndexSettings]
}

case class Type(`type`: String)

object Type {
  implicit val _: Format[Type] = Json.format
}

case class PropertiesWrapper(properties: Map[String, Type])

object PropertiesWrapper {
  def apply(headerMappings: List[(String, String)]): PropertiesWrapper = {
    val properties = headerMappings.foldLeft(Map.empty[String, Type]) {
      (headerTypeMap, headerMappingsTuple) =>
        headerTypeMap + (headerMappingsTuple._1 -> Type(headerMappingsTuple._2))
    }
    new PropertiesWrapper(properties)
  }

  implicit val _: Format[PropertiesWrapper] = Json.format
}

case class Mappings(typeName123456789012345678901234567890: PropertiesWrapper)

object Mappings {
  implicit val _: Format[Mappings] = Json.format
}

case class IndexAndMapping(settings: IndexSettings, mappings: Mappings) {
  def getJsonForIndexType(`type`: String) = Json.prettyPrint(Json.toJson(this))
    .replace("typeName123456789012345678901234567890", `type`)
}

case object IndexAndMapping {
  implicit val _: Format[IndexAndMapping] = Json.format
}

class ESIndexMappings {

  def createIndexAndMappings(indexName: String, typeName: String, mappingMap: Map[String, String])(implicit ws: WSClient): Future[WSResponse] = {

    val indexAndMappings = IndexAndMapping(IndexSettings(3, 2), Mappings(PropertiesWrapper(mappingMap.toList)))

    ws.url("http://localhost:9200/" + indexName)
      .addHttpHeaders("Content-Type" -> "application/json")
      .put(indexAndMappings.getJsonForIndexType(typeName))
  }

}
