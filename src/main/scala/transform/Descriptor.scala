package transform
import transform.Base_Reference
import mysql.Fiadmin
import config.Settings
import scala.io.Source
import scala.util.matching.Regex
import java.net.URLEncoder
import java.time._
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson._
import com.typesafe.scalalogging.Logger


/**
  * Transforms data into the Main.descriptor model standard
  */
class Descriptor extends Base_Reference:
  val logger = Logger("default")

  var docs = List[Document]()

  /**
    * Wrapper to transform all fields into the Main.descriptor model standard
    *
    * @param doc Document to be transformed
    * @param pk Primary Key for this document
    */
  def transform(doc: Document, pk: Int): List[Document] =
    this.doc = doc
    var values_v87 = get_all_values_as_document("87")
    var values_v88 = get_all_values_as_document("88")
    
    if (values_v87.size > 0 || values_v88.size > 0) {
      values_v87.toArray.foreach(value =>
        val descriptor_qualifier = get_descriptor_qualifier(value)
        val descriptor = descriptor_qualifier(0)
        val qualifier = descriptor_qualifier(1)

        if (descriptor != "") {
          val keyword_doc = transform_decs_keyword(pk, descriptor, qualifier, primary=true)
          this.docs = this.docs :+ keyword_doc
        }
      )
      
      values_v88.toArray.foreach(value =>
        val descriptor_qualifier = get_descriptor_qualifier(value)
        val descriptor = descriptor_qualifier(0)
        val qualifier = descriptor_qualifier(1)

        if (descriptor != "") {
          val keyword_doc = transform_decs_keyword(pk, descriptor, qualifier, primary=false)
          this.docs = this.docs :+ keyword_doc
        }
      )

      return this.docs
    } else {
      return null
    }

  /**
    * Extracts the descriptor and qualifier from a keyword entry
    *
    * @param keyword
    */
  def get_descriptor_qualifier(keyword: Object): List[String] =
    var value_doc = keyword.asInstanceOf[BsonDocument]

    var descriptor = ""
    var qualifier = ""
    if (value_doc.keySet.contains("_d") == true) {
      descriptor = value_doc.get("_d").asString.getValue().trim()
      if (descriptor != "") {
        if (value_doc.keySet.contains("_s") == true) {
          qualifier = value_doc.get("_s").asString.getValue().trim()
        }
      }
    } else if (value_doc.keySet.contains("text") == true) {
      val value_text = value_doc.get("text").asString.getValue().trim()
      if (value_text != "") {
        val text_split = value_text.split("/")

        descriptor = text_split(0)
        if (text_split.length > 1) {
          qualifier = text_split(1)
        }
      }
    }
    return List(descriptor, qualifier)

  /**
    * Sets BIREME's primary key
    *
    * @param pk
    */
  def set_object_id(pk: Int, fields: Document): Document =
    fields.put("object_id", pk)
    return fields

  /**
    * Sets the created and updated date and time with
    * timezone
    */
  def set_created_updated_datetime(fields: Document): Document =
    val tz_datetime = ZonedDateTime.now().toString()
    fields.put("created_time", tz_datetime)
    fields.put("updated_time", tz_datetime)
    return fields

  def transform_decs_keyword(pk: Int, descriptor: String, qualifier: String, primary: Boolean): Document =
    var decs_descriptor = descriptor
    var decs_codes = Fiadmin.get_decs_descriptor(descriptor)
    if (decs_codes != "" && qualifier != "") {
      val qualifier_result = Fiadmin.get_decs_qualifier(qualifier)
      if (qualifier_result != null) {
        decs_codes += qualifier_result(0)
        decs_descriptor += "/" + qualifier_result(1)
      } else {
        //val value_v2 = get_first_value("2")
        if (primary == true) {
          logger.warn(s"87,text,$qualifier")
        } else {
          logger.warn(s"88,text,$qualifier")
        }
      }
    }

    var fields = Document(
      "status" -> 1,
      "created_by" -> Settings.getConf("CREATED_BY").toInt,
      "content_type" -> 43,
      "primary" -> primary,
      "text" -> decs_descriptor,
      "code" -> decs_codes
    )
    var keyword_doc = Document(
      "model" -> "main.descriptor",
      "pk" -> BsonNull()
    )

    fields = this.set_object_id(pk, fields)
    fields = this.set_created_updated_datetime(fields)

    keyword_doc += ("fields", fields)

    return keyword_doc