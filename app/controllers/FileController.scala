package controllers

import java.nio.file.Paths

import javax.inject.Inject
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.io.Source

class FileController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  def readCSVFile() = Action(parse.multipartFormData){ implicit request =>

    val recordsJson = request.body.file("uploadedFile").map { uploadedFile =>

      val lines: Iterator[String] = Source.fromFile(uploadedFile.ref.getAbsoluteFile).getLines()

      val headers = lines.next().split(",").toList

      val recordsMap: List[Map[String, String]] = lines.drop(1).toList.map { line =>
        val records = line.split(",").toList
        (headers zip records).toMap
      }

      Json.prettyPrint(Json.toJson(recordsMap))
    }.getOrElse("{data: null}")

    println(recordsJson)

    Ok(recordsJson).as("application/json")
  }

  /**
    * The json can have multiple type:
    *   A String
    *   A number
    *   An object (can't be passed in a CSV data)
    *   An array
    *   A boolean
    *   A null
    *
    * --> The goal of this method is to create the JSON out of map so that
    * String values which can be number be converted to the number.
    */
  private def formTypedJson(jsonMap: Map[String, String]): Unit = {
    jsonMap
  }

  private def specializeRecord(recordList: List[String]): List[Any] = {
    recordList.map { strRecord =>
      if(strRecord.matches("([0-9]+)(.{1})([0-9]*)")) {
        strRecord.toDouble
      } else {
        strRecord
      }
    }
  }

}
