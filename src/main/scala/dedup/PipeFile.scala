package dedup

import java.io._
import java.nio.charset.StandardCharsets
import scala.concurrent.*
import scala.concurrent.duration.*

import mongo.MongoDB
import org.bson.BsonString
import org.bson.BsonDocument
import org.mongodb.scala.bson.collection.mutable.Document
import org.mongodb.scala.ObservableFuture
import org.mongodb.scala.model.Filters._


/**
  * Creates the Pipe files for SAS, MNT and MNTam
  * types
  */
object PipeFile:
  var mongodb_transformed = MongoDB()
  mongodb_transformed.connect()
  mongodb_transformed.set_collection("02_transformed")

  def get_first_value(document : BsonDocument, key: String, subfield: String = "text"): String =
    var field_value: String = ""

    if (document.keySet.contains(key) == true) {
      if (document.get(key).isDocument()) {
        field_value = document.get(key).asDocument().getString(subfield, BsonString("")).getValue().trim()
      } else if (document.get(key).isArray()) {
        document.get(key).asArray().forEach(row =>
          val occ_value = row.asDocument().getString(subfield, BsonString("")).getValue().trim()
          if (occ_value != "") {
            if (field_value != "") {
              field_value += "//@//"
            }
            field_value += occ_value
          }
        )
      } else {
        field_value = document.getString(key).getValue().trim()
      }
    }

    //field_value = new String(field_value.getBytes("ISO-8859-1"), "ISO-8859-1")
    return field_value


  def create_mnt_pipe(in_pipe_path : String) : Unit =
    var docs = mongodb_transformed.collection.find(
      and(
        or(
          equal("reference.fields.literature_type", "M"),
          equal("reference.fields.literature_type", "N"),
          equal("reference.fields.literature_type", "T"),
        ),
        regex("reference.fields.treatment_level", "m.*")
      )
    )

    val writer = new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream(in_pipe_path), 
        StandardCharsets.UTF_8
      )
    )
    var processing = true
    docs.subscribe(
      (doc: Document) => {
        val reference = doc.get("reference").get.asDocument()
        val reference_fields = reference.get("fields").asDocument()

        var title_monographic = ""
        var pages_monographic = ""
        var author_monographic = ""
        if (doc.keySet.contains("referencesource") == true) {
          val referencesource = doc.get("referencesource").get.asDocument()
          if (referencesource != null) {
            val referencesource_fields = referencesource.get("fields").asDocument()

            title_monographic = get_first_value(referencesource_fields, "title_monographic")
            pages_monographic = get_first_value(referencesource_fields, "pages_monographic")

            val individual_author_monographic = get_first_value(referencesource_fields, "individual_author_monographic")
            if (individual_author_monographic != "") {
              author_monographic = individual_author_monographic
            } else {
              val corporate_author_monographic = get_first_value(referencesource_fields, "corporate_author_monographic")
              author_monographic = corporate_author_monographic
            }
          }
        }

        val id = reference.getInt32("pk").getValue()
        val publication_date_normalized = get_first_value(reference_fields, "publication_date_normalized").slice(0,4)
        
        writer.write(s"mnt|$id|$title_monographic|$publication_date_normalized|$author_monographic|$pages_monographic\n")
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }
    writer.close()


  def create_mntam_pipe(in_pipe_path : String) : Unit =
    var docs = mongodb_transformed.collection.find(
      and(
        or(
          equal("reference.fields.literature_type", "M"),
          equal("reference.fields.literature_type", "N"),
          equal("reference.fields.literature_type", "T"),
        ),
        regex("reference.fields.treatment_level", "a.*")
      )
    )

    val writer = new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream(in_pipe_path), 
        StandardCharsets.UTF_8
      )
    )
    var processing = true
    docs.subscribe(
      (doc: Document) => {
        val reference = doc.get("reference").get.asDocument()
        val reference_fields = reference.get("fields").asDocument()

        var title_monographic = ""
        if (doc.keySet.contains("referencesource")) {
          val referencesource = doc.get("referencesource").get.asDocument()
          if (referencesource != null) {
            val referencesource_fields = referencesource.get("fields").asDocument()

            title_monographic = get_first_value(referencesource_fields, "title_monographic")
          }
        }
        
        var title = ""
        var pages = ""
        var author = ""
        if (doc.keySet.contains("referenceanalytic")) {
          val referenceanalytic = doc.get("referenceanalytic").get.asDocument()
          if (referenceanalytic != null) {
            val referenceanalytic_fields = referenceanalytic.get("fields").asDocument()

            title = get_first_value(referenceanalytic_fields, "title")
            pages = get_first_value(referenceanalytic_fields, "pages")

            val individual_author = get_first_value(referenceanalytic_fields, "individual_author")
            if (individual_author != "") {
              author = individual_author
            } else {
              val corporate_author = get_first_value(referenceanalytic_fields, "corporate_author")
              author = corporate_author
            }
          }
        }

        val id = reference.getInt32("pk").getValue()
        val publication_date_normalized = get_first_value(reference_fields, "publication_date_normalized").slice(0,4)
        
        writer.write(s"mntam|$id|$title|$title_monographic|$publication_date_normalized|$author|$pages\n")
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }
    writer.close()


  def create_sas_seven_pipe(in_pipe_path : String) : Unit =
    var docs = mongodb_transformed.collection.find(
      regex("reference.fields.literature_type", "^S.*")
    )

    val writer = new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream(in_pipe_path), 
        StandardCharsets.UTF_8
      )
    )
    var processing = true
    docs.subscribe(
      (doc: Document) => {
        val reference = doc.get("reference").get.asDocument()
        val reference_fields = reference.get("fields").asDocument()

        var title_serial = ""
        var volume_serial = ""
        var issue_number = ""
        if (doc.keySet.contains("referencesource")) {
          val referencesource = doc.get("referencesource").get.asDocument()
          if (referencesource != null) {
            val referencesource_fields = referencesource.get("fields").asDocument()

            title_serial = get_first_value(referencesource_fields, "title_serial")
            volume_serial = get_first_value(referencesource_fields, "volume_serial")
            issue_number = get_first_value(referencesource_fields, "issue_number")
          }
        }
        
        var title = ""
        var pages = ""
        var author = ""
        if (doc.keySet.contains("referenceanalytic")) {
          val referenceanalytic = doc.get("referenceanalytic").get.asDocument()
          if (referenceanalytic != null) {
            val referenceanalytic_fields = referenceanalytic.get("fields").asDocument()

            title = get_first_value(referenceanalytic_fields, "title")
            pages = get_first_value(referenceanalytic_fields, "pages")
            
            val individual_author = get_first_value(referenceanalytic_fields, "individual_author")
            if (individual_author != "") {
              author = individual_author
            } else {
              val corporate_author = get_first_value(referenceanalytic_fields, "corporate_author")
              author = corporate_author
            }
          }
        }

        val id = reference.getInt32("pk").getValue()
        val publication_date_normalized = get_first_value(reference_fields, "publication_date_normalized").slice(0,4)
        
        writer.write(s"sasseven|$id|$title|$title_serial|$publication_date_normalized|$volume_serial|$issue_number|$author|$pages\n")
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }
    writer.close()


  def create_sas_five_pipe(in_pipe_path : String) : Unit =
    var docs = mongodb_transformed.collection.find(
      regex("reference.fields.literature_type", "^S.*")
    )

    val writer = new BufferedWriter(
      new OutputStreamWriter(
        new FileOutputStream(in_pipe_path), 
        StandardCharsets.UTF_8
      )
    )
    var processing = true
    docs.subscribe(
      (doc: Document) => {
        val reference = doc.get("reference").get.asDocument()
        val reference_fields = reference.get("fields").asDocument()

        var title_serial = ""
        var volume_serial = ""
        var issue_number = ""
        if (doc.keySet.contains("referencesource")) {
          val referencesource = doc.get("referencesource").get.asDocument()
          if (referencesource != null) {
            val referencesource_fields = referencesource.get("fields").asDocument()

            title_serial = get_first_value(referencesource_fields, "title_serial")
            volume_serial = get_first_value(referencesource_fields, "volume_serial")
            issue_number = get_first_value(referencesource_fields, "issue_number")
          }
        }
        
        var title = ""
        if (doc.keySet.contains("referenceanalytic")) {
          val referenceanalytic = doc.get("referenceanalytic").get.asDocument()
          if (referenceanalytic != null) {
            val referenceanalytic_fields = referenceanalytic.get("fields").asDocument()

            title = get_first_value(referenceanalytic_fields, "title")
          }
        }

        val id = reference.getInt32("pk").getValue()
        val publication_date_normalized = get_first_value(reference_fields, "publication_date_normalized").slice(0,4)
        
        writer.write(s"sasfive|$id|$title|$title_serial|$publication_date_normalized|$volume_serial|$issue_number\n")
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }
    writer.close()