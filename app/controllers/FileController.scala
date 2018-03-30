package controllers

import java.nio.file.Paths

import es.ESIndexMappings
import javax.inject.Inject
import play.api.libs.json.{Format, Json}
import play.api.libs.ws._
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.annotation.tailrec
import scala.io.Source

class FileController @Inject()(cc: ControllerComponents, wsClient: WSClient, esIndexMappings: ESIndexMappings)(implicit ws: WSClient = wsClient)
  extends AbstractController(cc) {

  def readCSVFile() = Action(parse.multipartFormData) { implicit request =>

    val recordsJson = request.body.file("uploadedFile").map { uploadedFile =>

      val lines: Iterator[String] = Source.fromFile(uploadedFile.ref.getAbsoluteFile).getLines()

      val headers = lines.next().split(",").toList

      val recordsMap: List[Map[String, String]] = lines.drop(1).toList.map { line =>
        val records = line.split(",").toList
        (headers zip records).toMap
      }

      val headerTypes = generateHeaderTypes(headers, recordsMap) //Use header types in ES

      esIndexMappings.createIndexAndMappings("csv_data", Paths.get(uploadedFile.filename).getFileName.toString.toLowerCase, headerTypes)

      Json.prettyPrint(Json.toJson(headerTypes))
    }.getOrElse("{data: null}")

    Ok(recordsJson).as("application/json")
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
    * --> The goal of this method is to create the JSON out of map so that
    * String values which can be number be converted to the number.
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

  def isLongRecord(recordOptValue: Option[String]): Boolean = {
    val doublePattern = "\\d+"
    recordOptValue.getOrElse("%").trim.matches(doublePattern)
  }

  def isDoubleRecord(recordOptValue: Option[String]): Boolean = {
    val doublePattern = "((\\d+)(\\.{1})(\\d+)?)|([0-9]+)|(([0])(\\.)(\\d+))"
    recordOptValue.getOrElse("%").trim.matches(doublePattern)
  }

  def isBooleanRecord(recordOptValue: Option[String]): Boolean = {
    List("true", "false").contains(recordOptValue.getOrElse("").toLowerCase.trim)
  }

  @tailrec
  private def createStringToDoubleJsonMap(jsonMap: Map[String, String],
                                          resultMap: Map[String, Double] = Map.empty[String, Double]): Map[String, Double] = {
    if (jsonMap.isEmpty) {
      return resultMap
    }

    val firstKeyValue = jsonMap.toList.head

    if (firstKeyValue._2.matches("((\\d+)(\\.{1})?(\\d+))|(^(0)(.)(\\d+))")) {
      createStringToDoubleJsonMap(jsonMap.toList.drop(1).toMap, resultMap + (firstKeyValue._1 -> firstKeyValue._1.toDouble))
    } else {
      createStringToDoubleJsonMap(jsonMap.toList.drop(1).toMap, resultMap)
    }
  }

}
