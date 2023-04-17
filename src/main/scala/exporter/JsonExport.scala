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

  def create_file(filename : String, content : String) =
    val path = s"$export_path/$filename.json"

    val jsonfile = new File(path)
    val filewriter = new PrintWriter(jsonfile)
    filewriter.write(content)
    filewriter.close()

  def run() =
    var processing = true
    this.docs.subscribe(
      (doc: Document) => {
        val id = doc.get("_id").get.asObjectId.getValue.toString
        create_file(id, doc.toJson)
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }