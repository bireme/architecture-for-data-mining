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
import scribe.Logger


/**
  * Transforms data into the Main.descriptor model standard
  */
class Descriptor extends Base_Reference:
  val logger = Logger("main.descriptor")

  var docs = List[Document]()

  /**
    * Wrapper to transform all fields into the Main.descriptor model standard
    *
    * @param doc Document to be transformed
    * @param pk Primary Key for this document
    */
  def transform(doc: Document, pk: Int): List[Document] =
    this.doc = doc
    var values_v87 = get_all_values("87")
    var values_v88 = get_all_values("88")

    if (values_v87.length > 0 || values_v88.length > 0) {
      values_v87.foreach(value =>
        val keyword_doc = transform_decs_keyword(pk, value, primary=true)
        this.docs = this.docs :+ keyword_doc
      )

      values_v88.foreach(value =>
        val keyword_doc = transform_decs_keyword(pk, value, primary=false)
        this.docs = this.docs :+ keyword_doc
      )
      
      return this.docs
    } else {
      return null
    }

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

  /**
    * Leverage DeCS WS to fetch the code for a given keyword
    */
  def get_decs_code(keyword: String): String =
    val decs_url = Settings.getConf("DECS_WS_URL") + URLEncoder.encode(keyword, "ISO-8859-1")
    var xml = ""
    var decs_code = ""
    val keyword_mfn_pattern: Regex = """db=\"decs\" mfn=\"(\d+)\">""".r

    try {
      xml = Source.fromURL(decs_url)("ISO-8859-1").mkString
    } catch {
      case e: Exception => e.printStackTrace
    }

    for patternMatch <- keyword_mfn_pattern.findAllMatchIn(xml) do
      decs_code = patternMatch.group(1)

    if (decs_code != "") {
      decs_code = "^d" + decs_code
    } else {
      val value_v2 = get_first_value("2")
      logger.warn(s"main.descriptor;$value_v2;DeCS;Invalid DeCS keyword - $keyword")
    }

    return decs_code
  
  def transform_decs_keyword(pk: Int, value: String, primary: Boolean): Document =
    var fields = Document(
      "status" -> Settings.getConf("STATUS").toInt,
      "created_by" -> Settings.getConf("CREATED_BY").toInt,
      "content_type" -> 43,
      "primary" -> primary,
      "text" -> value,
      "code" -> this.get_decs_code(value)
    )
    var keyword_doc = Document(
      "model" -> "main.descriptor",
      "pk" -> BsonNull()
    )

    fields = this.set_object_id(pk, fields)
    fields = this.set_created_updated_datetime(fields)

    keyword_doc += ("fields", fields)

    return keyword_doc