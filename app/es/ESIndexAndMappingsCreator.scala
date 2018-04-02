package es

import play.api.libs.json._
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.{ExecutionContext, Future}

case class IndexSettings(number_of_shards: Int, number_of_replicas: Int)

object IndexSettings {
  implicit val _: Format[IndexSettings] = Json.format[IndexSettings]
}

case class Type(`type`: String)

object Type {
  implicit val _: OFormat[Type] = Json.format
}

case class PropertiesWrapper(properties: Map[String, Type])

object PropertiesWrapper {
  def apply(headerMappings: List[(String, String)]): PropertiesWrapper = {
    val properties = headerMappings.foldLeft(Map.empty[String, Type]) ((headerTypeMap, headerMappingsTuple) =>
        headerTypeMap + (headerMappingsTuple._1 -> Type(headerMappingsTuple._2)))
    new PropertiesWrapper(properties)
  }

  implicit val _: OFormat[PropertiesWrapper] = Json.format
}

case class Mappings(typeName: PropertiesWrapper)

object Mappings {
  implicit val _: OFormat[Mappings] = Json.format
}

case class IndexAndMapping(settings: IndexSettings, mappings: Mappings) {
  def getJsonForIndexType(typeName: String) = {
    val jsonMap = Json.toJsObject(this).value
    val mappings = Json.toJsObject(jsonMap("mappings").as[Mappings]).value
    val updatedMappings = mappings + ((typeName, mappings("typeName"))) - "typeName"
    val updatedJsonMap = jsonMap ++ updatedMappings - "mappings"
    Json.prettyPrint(Json.toJson(updatedJsonMap))
  }
}

object IndexAndMapping {
  implicit val _: OFormat[IndexAndMapping] = Json.format
}

class ESIndexAndMappingsCreator {

  def createIndexAndMappings(indexName: String, typeName: String, mappingMap: Map[String, String])
                            (implicit ws: WSClient, ec: ExecutionContext): Future[WSResponse] = {
    val indexAndMappings = IndexAndMapping(IndexSettings(3, 2), Mappings(PropertiesWrapper(mappingMap.toList)))
    ws.url("http://localhost:9200/" + indexName)
      .addHttpHeaders("Content-Type" -> "application/json")
      .put(indexAndMappings.getJsonForIndexType(typeName))
  }

}

object ESIndexAndMappingsCreator {
  def deleteIndex(indexName: String)(implicit ws: WSClient, ec: ExecutionContext): Future[WSResponse] = {
    ws.url("http://localhost:9200/" + indexName)
      .addHttpHeaders("Content-Type" -> "application/json")
      .delete()
  }
}
