package exporter

import mongo.MongoDB
import org.mongodb.scala.bson.collection.mutable.Document
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.FileOutputStream
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

    val os = new FileOutputStream(path);
    val filewriter = new PrintWriter(new OutputStreamWriter(os, "UTF-8"))
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

        var references = "["
        if (doc.keySet.contains("referenceduplicate")) {
          references += doc.get("referenceduplicate").get.asDocument.toJson
        } else {
          if (doc.keySet.contains("reference")) {
            references += doc.get("reference").get.asDocument.toJson
          }
          if (doc.keySet.contains("referenceanalytic")) {
            references += ", " + doc.get("referenceanalytic").get.asDocument.toJson
          }
          if (doc.keySet.contains("referencesource")) {
            references += ", " + doc.get("referencesource").get.asDocument.toJson
          }
          if (doc.keySet.contains("referencecomplement")) {
            references += ", " + doc.get("referencecomplement").get.asDocument.toJson
          }
          if (doc.keySet.contains("referencelocal")) {
            references += ", " + doc.get("referencelocal").get.asDocument.toJson
          }
          if (doc.keySet.contains("descriptor")) {
            val descriptors = doc.get("descriptor").get.asArray
            descriptors.forEach(value =>
              references += ", " + value.asDocument.toJson
            )
          }
        }
        references += "]"

        create_file(id, references)
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }