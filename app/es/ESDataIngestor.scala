package es

import play.api.libs.json.{Format, Json}
import play.api.libs.ws.{WSClient, WSResponse}

import scala.concurrent.{ExecutionContext, Future}


case class IndexLocation(_index: String, _type: String, _id: String = null) //null is used here for Java compatibility
object IndexLocation {
  implicit val _: Format[IndexLocation] = Json.format
}

case class BulkIndex(index: IndexLocation)

object BulkIndex {
  implicit val _: Format[BulkIndex] = Json.format
}

class ESDataIngestor {
  def bulkIndex(indexName: String, indexType: String, recordsMap: List[Map[String, String]])
               (implicit ws: WSClient, ec: ExecutionContext): Future[WSResponse] = {
    val bulkIndexOperation = Json.stringify(Json.toJson(BulkIndex(IndexLocation(indexName, indexType))))
    val recordsMapBulkIndexDoc: String = recordsMap.foldLeft("")((bulkIndexJson, record) =>
      bulkIndexJson + bulkIndexOperation + "\n" + Json.stringify(Json.toJson(record)) + "\n"
    )

    ws.url("http://localhost:9200/_bulk")
      .addHttpHeaders("Content-Type" -> "application/x-ndjson")
      .post(recordsMapBulkIndexDoc)
  }
}
