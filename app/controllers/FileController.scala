package controllers

import java.nio.file.Paths

import es.{ESDataIngestor, ESIndexAndMappingsCreator}
import javax.inject.Inject
import play.api.libs.json.Json
import play.api.libs.ws._
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.control.NonFatal

class FileController @Inject()(cc: ControllerComponents,
                               wsClient: WSClient,
                               esIndexMappings: ESIndexAndMappingsCreator,
                               esIngester: ESDataIngestor)
                              (implicit ws: WSClient = wsClient,
                               ec: ExecutionContext) extends AbstractController(cc) {

  def readCSVFile() = Action.async(parse.multipartFormData) { implicit request =>

    request.body.file("uploadedFile") match {
      case Some(uploadedFile) =>
        val fileName = Paths.get(uploadedFile.filename).getFileName.toString.toLowerCase
        val lines: List[String] = Source.fromFile(uploadedFile.ref.getAbsoluteFile).getLines().toList
        val headers = lines.headOption.getOrElse("").split(",").toList

        val recordsMap: List[Map[String, String]] = lines.drop(1).map { line =>
          val records = line.split(",").toList
          (headers zip records).toMap
        }

        val indexName = "csv_data"
        val headerTypes = generateHeaderTypes(headers, recordsMap)

        esIndexMappings.createIndexAndMappings(indexName, fileName, headerTypes).flatMap(indexingResponse =>
          if (indexingResponse.status == 200 || indexingResponse.status == 201) {
            esIngester.bulkIndex(indexName, fileName, recordsMap).map(ingestionResponse =>
              if (ingestionResponse.status == 200 || ingestionResponse.status == 201) {
                Ok(Json.stringify(Json.toJson(headerTypes))).as("application/json")
              } else {
                BadRequest("{\"error\": \"Data couldn't be indexed in elastic search. HTTP status ES - " +
                  ingestionResponse.status + ". The index, though, is in there intact.\"}")
                  .as("application/json")
              }).recover {
              case NonFatal(ex: Exception) => ESIndexAndMappingsCreator.deleteIndex(indexName)
                BadRequest("{\"error\": \"Index couldn't be created in elastic search. HTTP status ES - "
                  + indexingResponse.status + "\"}").as("application/json")
            }
          } else {
            Future.successful(BadRequest("{\"error\": \"Index couldn't be created in elastic search. HTTP status ES - " +
              indexingResponse.status + ". If you've already ingested data then run the analytics over it. Or " +
              "rename your CSV file.\"}").as("application/json"))
          })

      case None => Future.successful(BadRequest("{\"error\" : \"The file hasn't been received or recieved partially.\""))
    }

  }

  /**
    * The json can have multiple type:
    * A String
    * A number
    * An object (can't be passed in a CSV data)
    * An array (A field may or may not have comma separated values. We will focus on no to implement for now.)
    * A boolean
    * A null
    *
    * @param headers The headers in the CSV file
    * @param records The records (rows) in CSV file
    */
  private def generateHeaderTypes(headers: List[String], records: List[Map[String, String]]): Map[String, String] = {
    def generateType(header: String, records: List[Map[String, String]]): String = {
      if (records.forall { singleRecordRow => isBooleanRecord(singleRecordRow.get(header)) }) {
        "boolean"
      } else if (records.forall(singleRecordRow => isLongRecord(singleRecordRow.get(header)))) {
        "long"
      } else if (records.forall(singleRecordRow => isDoubleRecord(singleRecordRow.get(header)))) {
        "double"
      } else {
        "text"
      }
    }

    headers.foldLeft(Map.empty[String, String])((headerTypeMap, header) =>
      headerTypeMap + (header -> generateType(header, records))
    )
  }

  private def isLongRecord(recordOptValue: Option[String]): Boolean = {
    val doublePattern = "\\d+"
    recordOptValue.getOrElse("%").trim.matches(doublePattern)
  }

  private def isDoubleRecord(recordOptValue: Option[String]): Boolean = {
    val doublePattern = "((\\d+)(\\.{1})(\\d+)?)|([0-9]+)|(([0])(\\.)(\\d+))"
    recordOptValue.getOrElse("%").trim.matches(doublePattern)
  }

  private def isBooleanRecord(recordOptValue: Option[String]): Boolean = {
    List("true", "false").contains(recordOptValue.getOrElse("").toLowerCase.trim)
  }

}
