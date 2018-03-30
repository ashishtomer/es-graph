package es

import play.api.libs.json.{Format, Json}
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.Future

case class IndexSettings(number_of_shards: Int, number_of_replicas: Int)

object IndexSettings {
  implicit val _: Format[IndexSettings] = Json.format
}

case class Type(`type`: String)

object Type {
  implicit val _: Format[Type] = Json.format
}

case class Mappings(mappings: String)

object Mappings {
  def generatedTypesForHeaders (headerMappings: Map[String, String] ): Map[String, Type] = {
    headerMappings.foldLeft (Map.empty[String, Type] ) {
      (headerTypeMap, headerMappingsTuple) =>
        headerTypeMap + (headerMappingsTuple._1 -> Type (headerMappingsTuple._2) )
    }
  }

  def apply(typeName: String, headerMappings: Map[String, String]): Mappings = {
    val properties = "{\n\"properties\" : " + Json.prettyPrint(Json.toJson(generatedTypesForHeaders(headerMappings))) + "\n}"
    new Mappings(s"{ ${typeName} : $properties }")
  }

  implicit val _: Format[Mappings] = Json.format[Mappings]
}

case class IndexAndMapping(settings: IndexSettings, mappings: Mappings)

case object IndexAndMapping {
  implicit val _: Format[IndexAndMapping] = Json.format
}

class ESIndexMappings {

  def createIndexAndMappings(indexName: String, typeName: String, mappingMap: Map[String, String])(implicit ws: WSClient): Future[WSResponse] = {
    val indexAndMappings = IndexAndMapping(IndexSettings(3, 2), Mappings(typeName, mappingMap))
    val indexAndMappingsJson = Json.prettyPrint(Json.toJson(indexAndMappings))

    println(indexAndMappingsJson)

    ws.url("localhost:9200/" + indexName).put(indexAndMappingsJson)
  }

}
