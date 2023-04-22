package transform
import config.Settings
import mongo.MongoDB
import transform._
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

  init_loggers()
  init_fiadmin_nextid()
  init_mongodb_collections()

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

    Logger("biblioref.referencecomplement")
    .withHandler(writer = FileWriter("logs" / "biblioref.referencecomplement.log"))
    .replace()

    Logger("biblioref.referencelocal")
    .withHandler(writer = FileWriter("logs" / "biblioref.referencelocal.log"))
    .replace()

    Logger("main.descriptor")
    .withHandler(writer = FileWriter("logs" / "main.descriptor.log"))
    .replace()

  /**
    * Init Fi-Admin's next id based on the last ID in their search index
    */
  def init_fiadmin_nextid() =
    val next_id_url = Settings.getConf("NEXT_ID_URL")
    fiadmin_nextid = Source.fromURL(next_id_url).mkString.toInt
    fiadmin_nextid += Settings.getConf("ID_OFFSET").toInt

  /**
    * Init all collections used in the transformer process to store
    * transformed data
    */
  def init_mongodb_collections() =
    mongodb_transformed.connect()

    mongodb_transformed.set_collection("02_transformed")
    mongodb_transformed.drop_collection()
    mongodb_transformed.set_collection("02_transformed")

    // Unique index
    val indexOptions = IndexOptions().unique(true)
    mongodb_transformed.collection.createIndex(Indexes.ascending("reference.pk"), indexOptions)
    mongodb_transformed.collection.createIndex(Indexes.ascending("referenceanalytic.pk"), indexOptions)
    mongodb_transformed.collection.createIndex(Indexes.ascending("referencesource.pk"), indexOptions)
    mongodb_transformed.collection.createIndex(Indexes.ascending("referencecomplement.fields.source"), indexOptions)
    mongodb_transformed.collection.createIndex(Indexes.ascending("referencelocal.fields.source"), indexOptions)
    mongodb_transformed.collection.createIndex(Indexes.ascending("descriptor.fields.object_id"), indexOptions)

  /**
    * Transforms all the ISIS docs into all Fi-Admin's models
    */
  def transform_docs() =
    var processing = true
    docs.subscribe(
      (doc: Document) => {
        val reference = Reference()
        val referenceanalytic = Reference_Analytic()
        val referencesource = Reference_Source()
        val referencecomplement = Reference_Complement()
        val referencelocal = Reference_Local()
        val descriptor = Descriptor()

        val reference_doc = reference.transform(doc, fiadmin_nextid)
        val referenceanalytic_doc = referenceanalytic.transform(doc, fiadmin_nextid)
        val referencesource_doc = referencesource.transform(doc, fiadmin_nextid)
        val referencecomplement_doc = referencecomplement.transform(doc, fiadmin_nextid)
        val referencelocal_doc = referencelocal.transform(doc, fiadmin_nextid)
        val descriptor_docs = descriptor.transform(doc, fiadmin_nextid)

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
          if (referencecomplement_doc != null) {
            merged_doc.put("referencecomplement", referencecomplement_doc)
          }
          if (referencelocal_doc != null) {
            merged_doc.put("referencelocal", referencelocal_doc)
          }
          if (descriptor_docs != null) {
            merged_doc.put("descriptor", descriptor_docs)
          }

          mongodb_transformed.insert_document(merged_doc)
          fiadmin_nextid += 1
        }
      },
      (e: Throwable) => {println(s"Error: $e")},
      () => {processing = false}
    )
    while (processing) {
      Thread.sleep(100)
    }