package transform
import config.Settings
import mongo.MongoDB
import transform.Reference
import transform.Reference_Analytic
import transform.Reference_Source
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.io.Source
import scribe.Logger
import scribe.file._

import ExecutionContext.Implicits.global

import org.mongodb.scala._
import org.mongodb.scala.bson._
import org.mongodb.scala.ObservableFuture
import org.mongodb.scala.model.IndexOptions
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.bson.collection.mutable.Document
import scala.collection.JavaConverters._


/**
  * Responsible for the data transformation in each field
  * Collections prefix created here is: 02_
  * Each method will create a "model" collection to be later
  * exported into JSON files
  */
object Transformer:
  var fiadmin_nextid: Int = _
  var mongodb_transformed = MongoDB()

  var mongodb_isiscopy = MongoDB()
  mongodb_isiscopy.connect()
  mongodb_isiscopy.set_collection("01_isiscopy")
  var docs = mongodb_isiscopy.collection.find()    

  this.init_loggers()
  this.init_fiadmin_nextid()
  this.init_mongodb_collections()

  /**
    * Init all loggers used in the transformer process
    */
  def init_loggers() =
    Logger("biblioref.reference")
    .withHandler(writer = FileWriter("logs" / "biblioref.reference.log"))
    .replace()

    Logger("biblioref.referenceanalytic")
    .withHandler(writer = FileWriter("logs" / "biblioref.referenceanalytic.log"))
    .replace()

    Logger("biblioref.referencesource")
    .withHandler(writer = FileWriter("logs" / "biblioref.referencesource.log"))
    .replace()

  /**
    * Init Fi-Admin's next id based on the last ID in their search index
    */
  def init_fiadmin_nextid() =
    val next_id_url = Settings.getConf("NEXT_ID_URL")
    this.fiadmin_nextid = Source.fromURL(next_id_url).mkString.toInt
    this.fiadmin_nextid += Settings.getConf("ID_OFFSET").toInt

  /**
    * Init all collections used in the transformer process to store
    * transformed data
    */
  def init_mongodb_collections() =
    this.mongodb_transformed.connect()

    this.mongodb_transformed.set_collection("02_transformed")
    this.mongodb_transformed.drop_collection()
    this.mongodb_transformed.set_collection("02_transformed")

    // Unique index
    val indexOptions = IndexOptions().unique(true)
    this.mongodb_transformed.collection.createIndex(Indexes.ascending("reference.pk"), indexOptions)
    this.mongodb_transformed.collection.createIndex(Indexes.ascending("referenceanalytic.fields.source"), indexOptions)

  /**
    * Transforms all the ISIS docs into all Fi-Admin's models
    */
  def transform_docs() =
    this.docs.subscribe(
      (doc: Document) => {
        val reference = Reference()
        val referenceanalytic = Reference_Analytic()
        val referencesource = Reference_Source()

        val reference_doc = reference.transform(doc, this.fiadmin_nextid)
        val referenceanalytic_doc = referenceanalytic.transform(doc, this.fiadmin_nextid)
        val referencesource_doc = referencesource.transform(doc, this.fiadmin_nextid)

        if (reference_doc != null) {
          var merged_doc = Document(
            "reference" -> reference_doc
          )

          if (referenceanalytic_doc != null) {
            merged_doc.put("referenceanalytic", referenceanalytic_doc)
          }
          if (referencesource_doc != null) {
            merged_doc.put("referencesource", referencesource_doc)
          }

          this.mongodb_transformed.insert_document(merged_doc)
          this.fiadmin_nextid += 1
        }
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {println("Done")}
    )
    Await.ready(this.docs.toFuture, 30.seconds)