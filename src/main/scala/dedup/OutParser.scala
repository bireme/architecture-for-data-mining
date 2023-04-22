package dedup

import util.control.Breaks._
import scala.io.Source
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.bson.BsonNull
import org.mongodb.scala.ObservableFuture
import org.mongodb.scala.bson.collection.mutable.Document
import scala.concurrent.*
import scala.concurrent.duration.*

import mongo.MongoDB


object OutParser:
  var mongodb_transformed = MongoDB()
  mongodb_transformed.connect()
  mongodb_transformed.set_collection("02_transformed")

  def parse_mnt(out_path : String, is_fiadmin : Boolean) =
    for (line <- Source.fromFile(out_path).getLines) {
      val line_arr = line.split('|')

      val title_sim = line_arr(10).trim.toDouble
      val year_sim = line_arr(14).trim.toDouble
      val author_sim = line_arr(18).trim.toDouble
      val page_sim = line_arr(22).trim.toDouble

      if (page_sim == 1.0 && year_sim == 1.0 && title_sim >= 0.8 && author_sim >= 0.8) {
        val doc1 = line_arr(4).trim.toInt
        val doc2 = line_arr(5).trim

        if (is_fiadmin) {
          set_doc_as_duplicate(doc1, doc2)
        } else {
          set_doc_as_duplicate(doc1)
          set_doc_as_duplicate(doc2.toInt)
        }
      }
    }

  def parse_mntam(out_path : String, is_fiadmin : Boolean) =
    for (line <- Source.fromFile(out_path).getLines) {
      val line_arr = line.split('|')

      val title_sim = line_arr(10).trim.toDouble
      val title2_sim = line_arr(14).trim.toDouble
      val year_sim = line_arr(18).trim.toDouble
      val author_sim = line_arr(22).trim.toDouble
      val page_sim = line_arr(26).trim.toDouble

      if (page_sim == 1.0 && year_sim == 1.0 && title_sim >= 0.8 && title2_sim >= 0.8 && author_sim >= 0.8) {
        val doc1 = line_arr(4).trim.toInt
        val doc2 = line_arr(5).trim

        if (is_fiadmin) {
          set_doc_as_duplicate(doc1, doc2)
        } else {
          set_doc_as_duplicate(doc1)
          set_doc_as_duplicate(doc2.toInt)
        }
      }
    }

  def parse_sas7(out_path : String, is_fiadmin : Boolean) =
    for (line <- Source.fromFile(out_path).getLines) {
      val line_arr = line.split('|')

      val title_sim = line_arr(10).trim.toDouble
      val title2_sim = line_arr(14).trim.toDouble
      val year_sim = line_arr(18).trim.toDouble
      val vol_sim = line_arr(22).trim.toDouble
      val number_sim = line_arr(26).trim.toDouble
      val author_sim = line_arr(30).trim.toDouble
      val page_sim = line_arr(34).trim.toDouble

      if (page_sim == 1.0 && year_sim == 1.0 && vol_sim == 1.0 && number_sim == 1.0 && 
        title_sim >= 0.8 && title2_sim >= 0.9 && author_sim >= 0.8) {

        val doc1 = line_arr(4).trim.toInt
        val doc2 = line_arr(5).trim

        if (is_fiadmin) {
          set_doc_as_duplicate(doc1, doc2)
        } else {
          set_doc_as_duplicate(doc1)
          set_doc_as_duplicate(doc2.toInt)
        }
      }
    }

  def parse_sas5(out_path : String, is_fiadmin : Boolean) =
    for (line <- Source.fromFile(out_path).getLines) {
      breakable {
        val line_arr = line.split('|')

        val title_sim = line_arr(10).trim.toDouble
        val title2_sim = line_arr(14).trim.toDouble
        val year_sim = line_arr(18).trim.toDouble
        val vol_sim = line_arr(22).trim.toDouble
        val number_sim = line_arr(26).trim.toDouble

        var sim_threshold = 3.0
        if (line_arr(20).trim != "" && line_arr(21).trim != "") {
          sim_threshold = 2.8
          if (vol_sim != 1.0) {
            break
          }
        }
        if (line_arr(24).trim != "" && line_arr(25).trim != "") {
          sim_threshold = 2.8
          if (number_sim != 1.0) {
            break
          }
        }

        if (title_sim >= 0.8 && title2_sim == 1.0 && year_sim == 1.0 && 
            (title_sim + title2_sim + year_sim) >= sim_threshold) {

          val doc1 = line_arr(4).trim.toInt
          val doc2 = line_arr(5).trim

          if (is_fiadmin) {
            set_doc_as_duplicate(doc1, doc2)
          } else {
            set_doc_as_duplicate(doc1)
            set_doc_as_duplicate(doc2.toInt)
          }
        }
      }
    }

  def remove_field(doc_id : Integer, field_name : String) =
    var update = mongodb_transformed.collection.updateOne(
      and(
        equal("reference.pk", doc_id),
        exists(field_name, true)
      ),
      unset(field_name)
    )

    //var processing = true
    update.subscribe(
      Void => {},
      (e: Throwable) => {println(s"Error: $e")},
      //() => {processing = false}
      () => {}
    )
    Await.ready(update.toFuture, 30.seconds)
    /*while (processing) {
      Thread.sleep(100)
    }*/

  def set_doc_as_duplicate(doc_id : Integer, id_fiadmin : String = null) =
    var doc = mongodb_transformed.collection.find(
      and(
        equal("reference.pk", doc_id),
        exists("referenceduplicate", false)
      )
    ).first()

    var processing = true
    doc.subscribe(
      (obj: Document) => {
        var fields = Document()
        var new_doc = Document(
          "model" -> "biblioref.referenceduplicate",
          "pk" -> BsonNull()
        )

        if (id_fiadmin == null) {
          new_doc.put("reference", BsonNull())
        } else {
          new_doc.put("reference", id_fiadmin)
        }

        val reference = obj.get("reference").get.asDocument()
        val reference_fields = reference.get("fields").asDocument()

        if (reference_fields.keySet.contains("cooperative_center_code") == true) {
          new_doc.put("cooperative_center_code", reference_fields.get("cooperative_center_code"))
        }

        var metadata_fields = Document()
        var reference_fields_clone = reference_fields.clone
        if (obj.keySet.contains("referencesource") == true) {
          val referencesource = obj.get("referencesource").get.asDocument()
          val referencesource_fields = referencesource.get("fields").asDocument()

          referencesource_fields.keySet.forEach(key =>
            if (reference_fields_clone.keySet.contains(key) == false) {
              reference_fields_clone.put(key, referencesource_fields.get(key))
            }
          )
        }
        if (obj.keySet.contains("referenceanalytic") == true) {
          val referenceanalytic = obj.get("referenceanalytic").get.asDocument()
          val referenceanalytic_fields = referenceanalytic.get("fields").asDocument()

          referenceanalytic_fields.keySet.forEach(key =>
            if (reference_fields_clone.keySet.contains(key) == false) {
              reference_fields_clone.put(key, referenceanalytic_fields.get(key))
            }
          )
        }
        metadata_fields.put("fields", reference_fields_clone)
        fields.put("metadata_json", metadata_fields)


        if (obj.keySet.contains("referencecomplement") == true) {
          val referencecomplement = obj.get("referencecomplement").get.asDocument()
          val referencecomplement_fields = referencecomplement.get("fields").asDocument()

          var complement_fields = Document()
          complement_fields.put("fields", referencecomplement_fields)
          fields.put("complement_json", complement_fields)
        }


        if (obj.keySet.contains("referencelocal") == true) {
          val referencelocal = obj.get("referencelocal").get.asDocument()
          val referencelocal_fields = referencelocal.get("fields").asDocument()

          var library_fields = Document()
          library_fields.put("fields", referencelocal_fields)
          fields.put("library_json", library_fields)
        }


        if (obj.keySet.contains("descriptor") == true) {
          val descriptors = obj.get("descriptor").get.asArray()

          var all_fields = List[Document]()
          descriptors.forEach(descriptor =>
            val descriptor_fields = descriptor.asDocument.get("fields").asDocument()

            var indexing_fields = Document()
            indexing_fields.put("fields", descriptor_fields)
            all_fields = all_fields :+ indexing_fields
          )
          fields.put("indexing_json", all_fields)
        }

        new_doc += ("fields", fields)

        obj.put("referenceduplicate", new_doc)

        mongodb_transformed.collection.updateOne(
          equal("reference.pk", doc_id),
          Document("$set" -> obj)
        ).toFuture()
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }

    remove_field(doc_id, "referencesource")
    remove_field(doc_id, "referenceanalytic")
    remove_field(doc_id, "referencecomplement")
    remove_field(doc_id, "referencelocal")
    remove_field(doc_id, "descriptor")
    remove_field(doc_id, "reference")