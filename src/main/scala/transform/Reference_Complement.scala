package transform
import transform.Base_Reference
import mysql.Fiadmin
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson._
import scribe.Logger


/**
  * Transforms data into the Biblioref.referencecomplement model standard
  */
class Reference_Complement extends Base_Reference:
    val logger = Logger("biblioref.referencecomplement")

    fields = Document()
    new_doc = Document(
      "model" -> "biblioref.referencecomplement",
      "pk" -> BsonNull()
    )

    /**
      * Wrapper to transform all fields into the Biblioref.referencecomplement model standard
      *
      * @param doc Document to be transformed
      * @param pk Primary Key for this document
      */
    def transform(doc: Document, pk: Int): Document =
      this.doc = doc
      val value_v53 = get_first_value("53")
      val value_v59 = get_first_value("59")

      if (value_v53 != "" || value_v59 != "") {
        this.set_source(pk)

        this.set_field_as_string("conference_sponsoring_institution", "52")
        this.set_field_as_string("conference_name", "53")
        this.set_field_as_string("conference_date", "54")
        this.set_field_as_string("conference_normalized_date", "55")
        this.set_field_as_string("conference_city", "56")
        this.set_field_as_string("project_name", "58")
        this.set_field_as_string("project_sponsoring_institution", "59")
        this.set_field_as_string("project_number", "60")

        this.transform_conference_country()

        this.new_doc += ("fields", this.fields)
        return this.new_doc
      } else {
        return null
      }

    /**
      * Sets BIREME's primary key
      *
      * @param pk
      */
    def set_source(pk: Int) : Unit =
      this.fields.put("source", pk)

    /**
      * Transforms the "conference_country" field for FI-Admin.
      * Replaces the content by the country code in FI-Admin.
      */
    def transform_conference_country() : Unit =
      var values_v57 = get_all_values("57")
      
      var i = 0;
      values_v57.toArray.foreach(value =>
        val value_v57 = value.trim()
        if (value_v57 != "") {
          val country_code = Fiadmin.get_country_code(value_v57)
          if (country_code == null) {
            val value_v2 = get_first_value("2")
            logger.warn(s"biblioref.referencecomplement;$value_v2;v57p;Not found in FI Admin - $value_v57")
            values_v57 = values_v57.updated(i, "")
          } else {
            values_v57 = values_v57.updated(i, value_v57)
          }
        }
        i += 1
      )

      this.fields.put("conference_country", values_v57)