package exporter

import mongo.MongoDB
import org.mongodb.scala.bson.collection.mutable.Document
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import java.io.File
import java.io.PrintWriter
import org.bson.BsonString


object JsonExport:
  val project_path = Paths.get(".").toAbsolutePath
  val export_path = s"$project_path/export"

  var mongodb_transformed = MongoDB()
  mongodb_transformed.connect()
  mongodb_transformed.set_collection("02_transformed")
  var docs = mongodb_transformed.collection.find()

  def create_file(filename : String, content : String) : Unit =
    val path = s"$export_path/$filename.json"

    val jsonfile = new File(path)
    val filewriter = new PrintWriter(jsonfile)
    filewriter.write(content)
    filewriter.close()

  /**
    * Exports all documents in JSON format
    */
  def run() : Unit =
    var processing = true
    docs.subscribe(
      (doc: Document) => {
        val id = doc.get("_id").get.asObjectId.getValue.toString

        // Remove referencesource if "source" exists in referenceanalytic
        if (doc.keySet.contains("referenceanalytic")) {
          val analytic = doc.get("referenceanalytic").get.asDocument
          val analytic_fields = analytic.get("fields").asDocument
          if (analytic_fields.keySet.contains("source")) {
            doc.remove("referencesource")
          }
        }

        create_file(id, doc.toJson)
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }