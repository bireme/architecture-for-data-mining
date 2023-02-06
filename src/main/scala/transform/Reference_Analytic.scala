package transform
import transform.Base_Reference
import mysql.Fiadmin
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson._
import scribe.Logger


/**
  * Transforms data into the Biblioref.referenceanalytic model standard
  */
class Reference_Analytic extends Base_Reference:
    val logger = Logger("biblioref.referenceanalytic")

    fields = Document()
    new_doc = Document(
      "model" -> "biblioref.referenceanalytic"
    )

    /**
      * Wrapper to transform all fields into the Biblioref.referenceanalytic model standard
      *
      * @param doc Document to be transformed
      * @param pk Primary Key for this document
      */
    def transform(doc: Document, pk: Int): Document =
      this.doc = doc
      val value_v6 = get_first_value("6")
      if (value_v6.contains("a")) {
        // This should be the first Analytic PK, the one from the same referencesource
        //this.set_source(pk)
        this.set_pk(pk)

        this.set_field_as_string("english_translated_title", "13")
        this.set_field_as_string("doi_number", "724")

        this.set_field_as_document("pages", "14")

        this.transform_title()
        this.transform_individual_author()
        this.transform_corporate_author()
        this.transform_clinical_trial_registry_name()

        this.new_doc += ("fields", this.fields)
        return this.new_doc
      } else {
        return null
      }
    
    //def set_source(pk: Int) =
      //this.fields.put("source", pk)

    /**
      * Sets BIREME's primary key
      *
      * @param pk
      */
    def set_pk(pk: Int) =
      this.new_doc += ("pk", pk)
    
    /**
      * Transforms the "clinical_trial_registry_name" field for FI-Admin.
      * Simply adds the content of the field v700 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_clinical_trial_registry_name() =
      this.set_field_as_string("clinical_trial_registry_name", "700")

      val values = get_all_values("700")
      val value_v2 = get_first_value("2")
      values.foreach(value =>
        val is_valid = Fiadmin.is_code_valid(value, "clinical_trial_database")
        if (!is_valid) {
          logger.warn(s"biblioref.referenceanalytic;$value_v2;v700;Not found in FI Admin - $value")
        }
      )

    /**
      * Transforms the "title" field for FI-Admin.
      * Simply adds the content of the field v12 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      * for the subfield _i
      */
    def transform_title() =
      this.set_field_as_document("title", "12")

      val value_v2 = get_first_value("2")
      val values = get_all_values("12", "_i")
      values.foreach(value =>
        val is_valid = Fiadmin.is_code_valid(value, "text_language")
        if (!is_valid) {
          logger.warn(s"biblioref.referenceanalytic;$value_v2;v12_i;Not found in FI Admin - $value")
        }
      )
    
    /**
      * Transforms the "corporate_author" field for FI-Admin.
      * Simply adds the content of the field v11 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      * for the subfield _r
      */
    def transform_corporate_author() =
      this.set_field_as_document("corporate_author", "11")

      val value_v2 = get_first_value("2")
      val values = get_all_values("11", "_r")
      values.foreach(value =>
        val is_valid = Fiadmin.is_code_valid(value, "degree_of_responsibility")
        if (!is_valid) {
          logger.warn(s"biblioref.referenceanalytic;$value_v2;v11_r;Not found in FI Admin - $value")
        }
      )

    /**
      * Transforms the "individual_author" field for FI-Admin.
      * Simply adds the content of the field v10 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      * for the subfield _r and _p
      */
    def transform_individual_author() =
      var values_v10 = get_all_values_as_document("10")
      if (values_v10.size > 0) {
        val value_v2 = get_first_value("2")

        /* Dealing with _r subfield */
        var values = get_all_values("10", "_r")
        values.foreach(value =>
          val is_valid = Fiadmin.is_code_valid(value, "degree_of_responsibility")
          if (!is_valid) {
            logger.warn(s"biblioref.referenceanalytic;$value_v2;v10_r;Not found in FI Admin - $value")
          }
        )

        /* Dealing with _p subfield and replacing the country name by its country code */
        var i = 0;
        values_v10.toArray.foreach(value =>
          var value_doc = value.asInstanceOf[BsonDocument]
          if (value_doc.keySet.contains("_p") == true) {
            val value_v10_p = value_doc.get("_p").asString.getValue().trim()
            if (value_v10_p != "") {
              val country_code = Fiadmin.get_country_code(value_v10_p)
              if (country_code == null) {
                logger.warn(s"biblioref.referenceanalytic;$value_v2;v10_p;Not found in FI Admin - $value")
              } else {
                value_doc.remove("_p")
                value_doc.put("_p", BsonString(country_code))
                values_v10.set(i, value_doc)
              }
            }
          }
          i += 1
        )
        
        this.fields.put("individual_author", values_v10)
      }