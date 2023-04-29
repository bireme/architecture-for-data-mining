package dedup

import java.nio.file.Paths
import java.io.File
import mongo.MongoDB
import org.mongodb.scala.bson.collection.mutable.Document
import org.mongodb.scala.ObservableFuture
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._
import org.bson._
import org.bireme.dcdup.DoubleCheckDuplicated
import dedup.PipeFile
import dedup.OutParser


/**
  * Manages the Dedup process.
  * It will call for pile files creation, will run 
  * BIREME's DoubleCheckDuplicated on each pipe file
  * then will parse the output to flag the duplicate records
  * in MongoDB
  */
class Deduplication():
  val charset = "UTF-8"

  val project_path = Paths.get(".").toAbsolutePath
  val dedup_conf_path = s"$project_path/data/Dedup"
  val dedup_io_path = s"$project_path/temp"

  // DeDup piped input file
  val in_pipe_path = s"$dedup_io_path/in.pipe"

  // Config Sas Five fields
  val conf_sas_five_path = s"$dedup_conf_path/configs/configLILACS_Sas_Five.cfg"
  // Config Sas Seven fields
  val conf_sas_seven_path = s"$dedup_conf_path/configs/configLILACS_Sas_Seven.cfg"
  // Config Mnt Four fields
  val conf_mnt_path = s"$dedup_conf_path/configs/configLILACS_MNT_Four.cfg"
  // Config Mntam Five fields
  val conf_mntam_path = s"$dedup_conf_path/configs/configLILACS_MNTam_Five.cfg"

  // Index Sas
  val index_sas_path = s"$dedup_conf_path/indexes/lilacs_Sas"
  // Index Mnt
  val index_mnt_path = s"$dedup_conf_path/indexes/lilacs_MNT"
  // Index Mntam
  val index_mntam_path = s"$dedup_conf_path/indexes/lilacs_MNTam"

  // duplicated records found in pipe file
  val out_dup1_path = s"$dedup_io_path/out_dup1"
  // duplicated records found between pipe file and DeDup index
  val out_dup2_path = s"$dedup_io_path/out_dup2"
  // no duplicated records between input pipe file and Dedup index
  val out_nodup1_path = s"$dedup_io_path/out_nodup1"
  // no duplicated records between (pipe file and itself) and (pipe file and Dedup index)
  val out_nodup2_path = s"$dedup_io_path/out_nodup2"

  // Used for Source duplicate check
  var mongodb_transformed = MongoDB()
  mongodb_transformed.connect()
  mongodb_transformed.set_collection("02_transformed")

  override def toString: String = in_pipe_path

  /*
  * Erase the input and output files from BIREME's Dedup utility
  */
  def erase_in_out_files() : Unit =
    new File(in_pipe_path).delete()
    new File(out_dup1_path).delete()
    new File(out_dup2_path).delete()
    new File(out_nodup1_path).delete()
    new File(out_nodup2_path).delete()

  /**
  * Given the journal name, volume, number and pub_date will
  * fetch all documents that match these strings, will fetch the
  * first doc PK and set the "source" field in "referenceanalytic"
  */
  def set_source_field(duplicate_docs : org.mongodb.scala.FindObservable[Document]) : Unit =
    var processing = true
    var first_id : BsonValue = null
    duplicate_docs.subscribe(
      (doc: Document) => {
        if (first_id == null) {
          first_id = doc.get("reference").get.asDocument.get("pk")
        } else {
          val doc_id = doc.get("reference").get.asDocument.get("pk")
          doc.get("referenceanalytic").get.asDocument.get("fields").asDocument.put("source", first_id)

          mongodb_transformed.collection.updateOne(
            equal("reference.pk", doc_id),
            Document("$set" -> doc)
          ).toFuture()
        }
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }

  /**
  * Checks the "transformed" collection for SAS Source duplicates 
  * within the collection (through a 'group by' operation). 
  * Updates each duplicated record by creating the 'referenceanalytic.fields.source'
  * field pointing to the first record PK
  */
  def update_sas_source_duplicates() : Unit =
    var docs = mongodb_transformed.collection.aggregate(List(
      filter(
        and(
          exists("referenceanalytic"),
          exists("referenceanalytic.fields.source", false),
          exists("referencesource.fields.title_serial"),
          exists("reference.fields.publication_date_normalized")
        )
      ),
      group(
        List(
          "$referencesource.fields.title_serial", 
          "$referencesource.fields.volume_serial", 
          "$referencesource.fields.issue_number",
          "$reference.fields.publication_date_normalized"
        ), 
        sum("total", 1)
      ),
      filter(
        gte("total", 2)
      )
    ))

    var processing = true
    var i = 0
    docs.subscribe(
      (doc: Document) => {
        val fields = doc.get("_id").get.asArray
        
        var journal = fields.get(0)
        if (fields.get(0).isString) {
          journal = journal.asString
        } else {
          journal = BsonNull()
        }

        var volume = fields.get(1)
        if (fields.get(1).isString) {
          volume = volume.asString
        } else {
          volume = BsonNull()
        }

        var number = fields.get(2)
        if (fields.get(2).isString) {
          number = number.asString
        } else {
          number = BsonNull()
        }

        var pub_date = fields.get(3)
        if (fields.get(3).isString) {
          pub_date = pub_date.asString
        } else {
          pub_date = BsonNull()
        }

        var duplicate_docs = mongodb_transformed.collection.find(
          and(
            exists("referenceanalytic"),
            exists("referenceanalytic.fields.source", false),
            equal("referencesource.fields.title_serial", journal),
            equal("referencesource.fields.volume_serial", volume),
            equal("referencesource.fields.issue_number", number),
            equal("reference.fields.publication_date_normalized", pub_date)
          )
        )
        set_source_field(duplicate_docs)

        i += 1
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }

  /**
  * Checks the "transformed" collection for MNT Source duplicates 
  * within the collection (through a 'group by' operation). 
  * Updates each duplicated record by creating the 'referenceanalytic.fields.source'
  * field pointing to the first record PK
  */
  def update_mnt_source_duplicates() : Unit =
    var docs = mongodb_transformed.collection.aggregate(List(
      filter(
        and(
          exists("referenceanalytic"),
          exists("referenceanalytic.fields.source", false),
          exists("referencesource.fields.title_monographic"),
          exists("referencesource.fields.literature_type"),
          exists("reference.fields.publication_date_normalized"),
          regex("reference.fields.treatment_level", "^(?!am).*", "i")
        )
      ),
      group(
        List(
          "$referencesource.fields.literature_type", 
          "$referencesource.fields.title_monographic", 
          "$reference.fields.publication_date_normalized"
        ), 
        sum("total", 1)
      ),
      filter(
        gte("total", 2)
      )
    ))

    var processing = true
    var i = 0
    docs.subscribe(
      (doc: Document) => {
        val fields = doc.get("_id").get.asArray
        
        var literature_type = fields.get(0)
        if (literature_type.isString) {
          literature_type = literature_type.asString
        } else {
          literature_type = BsonNull()
        }

        var title_monographic = fields.get(1)
        if (title_monographic.isString) {
          title_monographic = title_monographic.asString
        } else {
          title_monographic = BsonNull()
        }

        var pub_date = fields.get(2)
        if (pub_date.isString) {
          pub_date = pub_date.asString
        } else {
          pub_date = BsonNull()
        }

        var duplicate_docs = mongodb_transformed.collection.find(
          and(
            exists("referenceanalytic"),
            exists("referenceanalytic.fields.source", false),
            equal("referencesource.fields.literature_type", literature_type),
            equal("referencesource.fields.title_monographic", title_monographic),
            equal("reference.fields.publication_date_normalized", pub_date)
          )
        )
        set_source_field(duplicate_docs)

        i += 1
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }

  def run() : Unit =
    // MNT
    erase_in_out_files()
    PipeFile.create_mnt_pipe(in_pipe_path)
    run_dedup(index_mnt_path, conf_mnt_path)
    OutParser.parse_mnt(out_dup2_path, true)
    OutParser.parse_mnt(out_dup1_path, false)

    // MNTAM
    erase_in_out_files()
    PipeFile.create_mntam_pipe(in_pipe_path)
    run_dedup(index_mntam_path, conf_mntam_path)
    OutParser.parse_mntam(out_dup2_path, true)
    OutParser.parse_mntam(out_dup1_path, false)

    // SAS 7
    erase_in_out_files()
    PipeFile.create_sas_seven_pipe(in_pipe_path)
    run_dedup(index_sas_path, conf_sas_seven_path)
    OutParser.parse_sas7(out_dup2_path, true)
    OutParser.parse_sas7(out_dup1_path, false)

    // SAS 5
    erase_in_out_files()
    PipeFile.create_sas_five_pipe(in_pipe_path)
    run_dedup(index_sas_path, conf_sas_five_path)
    OutParser.parse_sas5(out_dup2_path, true)
    OutParser.parse_sas5(out_dup1_path, false)

    // Source
    update_sas_source_duplicates()
    update_mnt_source_duplicates()

  def run_dedup(index_path : String, conf_path : String) : Unit =
    DoubleCheckDuplicated.doubleCheck(
      in_pipe_path,
      charset,
      index_path,
      conf_path,
      charset,
      out_dup1_path,
      out_dup2_path,
      out_nodup1_path,
      out_nodup2_path
    )