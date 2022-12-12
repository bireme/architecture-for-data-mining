package transform
import config.Settings
import mongo.MongoDB
import transform.Reference
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
class Transformer():
    Logger("biblioref.reference").withHandler(writer = FileWriter("logs" / "biblioref.reference.log")).replace()

    var mongodb_isiscopy = MongoDB()
    mongodb_isiscopy.connect()
    mongodb_isiscopy.set_collection("01_isiscopy")
    var docs = mongodb_isiscopy.collection.find()
    
    /**
      * Creates the Biblioref.reference model data to be
      * later exported
      */
    def create_biblioref_reference() =
        val mongodb_reference = MongoDB()
        mongodb_reference.connect()
        mongodb_reference.set_collection("02_biblioref.reference")
        mongodb_reference.drop_collection()
        mongodb_reference.set_collection("02_biblioref.reference")

        // Unique index
        val indexOptions = IndexOptions().unique(true)
        mongodb_reference.collection.createIndex(Indexes.ascending("pk"), indexOptions)

        val next_id_url = Settings.getConf("NEXT_ID_URL")
        var fiadmin_nextid = Source.fromURL(next_id_url).mkString.toInt
        fiadmin_nextid += 500
        
        this.docs.subscribe(
          (doc: Document) => {
            val reference = Reference()
            val new_doc = reference.transform(doc, fiadmin_nextid)
            if (new_doc != null) {
              mongodb_reference.insert_document(new_doc)
              fiadmin_nextid += 1
            }
          },
          (e: Throwable) => {println(s"Error: $e")},
          () => {println("Done")}
        )
        Await.ready(this.docs.toFuture, 30.seconds)