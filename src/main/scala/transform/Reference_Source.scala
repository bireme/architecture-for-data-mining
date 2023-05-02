package transform
import transform.Base_Reference
import mysql.Fiadmin
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson._
import scribe.Logger


/**
  * Transforms data into the Biblioref.referencesource model standard
  */
class Reference_Source extends Base_Reference:
  val logger = Logger("biblioref.referencesource")

  fields = Document()
  new_doc = Document(
    "model" -> "biblioref.referencesource"
  )

  /**
    * Wrapper to transform all fields into the Biblioref.referencesource model standard
    *
    * @param doc Document to be transformed
    * @param pk Primary Key for this document
    */
  def transform(doc: Document, pk: Int): Document =
    this.doc = doc
    
    set_pk(pk)

    set_field_as_string("english_title_monographic", "19")
    set_field_as_string("pages_monographic", "20")
    set_field_as_string("volume_monographic", "21")
    set_field_as_string("english_title_collection", "26")
    set_field_as_string("total_number_of_volumes", "27")
    set_field_as_string("volume_serial", "31")
    set_field_as_string("issue_number", "32")
    set_field_as_string("thesis_dissertation_institution", "50")
    set_field_as_string("publisher", "62")
    set_field_as_string("edition", "63")
    set_field_as_string("publication_city", "66")
    set_field_as_string("symbol", "68")
    set_field_as_string("isbn", "69")

    set_field_as_document("thesis_dissertation_leader", "49")

    transform_title_monographic()
    transform_title_collection()
    transform_corporate_author_monographic()
    transform_corporate_author_collection()
    transform_individual_author_monographic()
    transform_individual_author_collection()
    transform_thesis_dissertation_academic_title()
    transform_publication_country()
    transform_title_serial_and_issn()

    this.new_doc += ("fields", this.fields)
    return this.new_doc
  
  /**
    * Sets BIREME's primary key
    *
    * @param pk
    */
  def set_pk(pk: Int) : Unit =
    this.new_doc += ("pk", pk)

  /**
    * Transforms the "title_serial" and "issn" fields by looking up in FI-Admin database.
    * If not found, will push the content forward as available in the current database
    */
  def transform_title_serial_and_issn() : Unit =
    val value_v30 = get_first_value("30")
    val value_v35 = get_first_value("35")
    
    if (value_v30 != "" || value_v35 != "") {
      val value_v2 = get_first_value("2")

      val title_and_issn = Fiadmin.get_title_and_issn(value_v30, value_v35)
      if (title_and_issn(0) != "") {
        this.fields.put("title_serial", title_and_issn(0))
      } else {
        set_field_as_string("title_serial", "30")
        
        logger.warn(s"biblioref.referencesource;$value_v2;v30;Not found in FI Admin - $value_v30")
      }

      if (title_and_issn(1) != "") {
        this.fields.put("issn", title_and_issn(1))
      } else {
        set_field_as_string("issn", "35")

        logger.warn(s"biblioref.referencesource;$value_v2;v35;Not found in FI Admin - $value_v35")
      }
    }

  /**
    * Transforms the "publication_country" field for FI-Admin.
    * Replaces the content by the country code in FI-Admin.
    */
  def transform_publication_country() : Unit =
    var values_v67 = get_all_values("67")
    
    var i = 0
    values_v67.toArray.foreach(value =>
      val value_v67 = value.trim()
      if (value_v67 != "") {
        val country_code = Fiadmin.get_country_code(value_v67)
        if (country_code == null) {
          val value_v2 = get_first_value("2")
          logger.warn(s"biblioref.referencecomplement;$value_v2;v67;Not found in FI Admin - $value_v67")
          values_v67 = values_v67.updated(i, "")
        } else {
          values_v67 = values_v67.updated(i, value_v67)
        }
      }
      i += 1
    )

    this.fields.put("publication_country", values_v67)

  /**
    * Transforms the "thesis_dissertation_academic_title" field for FI-Admin.
    * Simply adds the content of the field v51 as is and issues
    * a warning if the code is not available in FI-Admin's MySQL database
    */
  def transform_thesis_dissertation_academic_title() : Unit =
    set_field_as_string("thesis_dissertation_academic_title", "51")

    val value_v51 = get_first_value("51")
    val is_valid = Fiadmin.is_code_valid(value_v51, "thesis_dissertation_academic_title")
    if (!is_valid) {
      val value_v2 = get_first_value("2")
      logger.warn(s"biblioref.referencesource;$value_v2;v51;Not found in FI Admin - $value_v51")
    }

  /**
    * Transforms the "title_monographic" field for FI-Admin.
    * Simply adds the content of the field v18 as is and issues
    * a warning if the code is not available in FI-Admin's MySQL database
    * for the subfield _i
    */
  def transform_title_monographic() : Unit =
    val value_v2 = get_first_value("2")
    val values = get_all_values_as_document("18")
    var i = 0
    values.forEach(value =>
      var value_doc = value.asDocument()
      if (value_doc.keySet.contains("_i") == true) {
        val subfield_value = value_doc.getString("_i", BsonString("")).getValue().trim()
        val is_valid = Fiadmin.is_code_valid(subfield_value, "text_language")
        if (!is_valid) {
          value_doc.remove("_i")
          value_doc.put("_i", BsonString(""))
          values.set(i, value_doc)
          
          logger.warn(s"biblioref.referencesource;$value_v2;v18_i;Not found in FI Admin - $value")
        }
      }
      i += 1
    )
    this.fields.put("title_monographic", values)

  /**
    * Transforms the "title_collection" field for FI-Admin.
    * Simply adds the content of the field v25 as is and issues
    * a warning if the code is not available in FI-Admin's MySQL database
    * for the subfield _i
    */
  def transform_title_collection() : Unit =
    val value_v2 = get_first_value("2")
    val values = get_all_values_as_document("25")
    var i = 0
    values.forEach(value =>
      var value_doc = value.asDocument()
      if (value_doc.keySet.contains("_i") == true) {
        val subfield_value = value_doc.getString("_i", BsonString("")).getValue().trim()
        val is_valid = Fiadmin.is_code_valid(subfield_value, "text_language")
        if (!is_valid) {
          value_doc.remove("_i")
          value_doc.put("_i", BsonString(""))
          values.set(i, value_doc)
          
          logger.warn(s"biblioref.referencesource;$value_v2;v25_i;Not found in FI Admin - $value")
        }
      }
      i += 1
    )
    this.fields.put("title_collection", values)

  /**
    * Transforms the "corporate_author_monographic" field for FI-Admin.
    * Simply adds the content of the field v17 as is and issues
    * a warning if the code is not available in FI-Admin's MySQL database
    * for the subfield _r
    */
  def transform_corporate_author_monographic() : Unit =
    val value_v2 = get_first_value("2")
    val values = get_all_values_as_document("17")
    var i = 0
    values.forEach(value =>
      var value_doc = value.asDocument()
      if (value_doc.keySet.contains("_r") == true) {
        val subfield_value = value_doc.getString("_r", BsonString("")).getValue().trim()
        val is_valid = Fiadmin.is_code_valid(subfield_value, "degree_of_responsibility")
        if (!is_valid) {
          value_doc.remove("_r")
          value_doc.put("_r", BsonString(""))
          values.set(i, value_doc)
          
          logger.warn(s"biblioref.referencesource;$value_v2;v17_r;Not found in FI Admin - $value")
        }
      }
      i += 1
    )
    this.fields.put("corporate_author_monographic", values)

  /**
    * Transforms the "corporate_author_collection" field for FI-Admin.
    * Simply adds the content of the field v24 as is and issues
    * a warning if the code is not available in FI-Admin's MySQL database
    * for the subfield _r
    */
  def transform_corporate_author_collection() : Unit =
    val value_v2 = get_first_value("2")
    val values = get_all_values_as_document("24")
    var i = 0
    values.forEach(value =>
      var value_doc = value.asDocument()
      if (value_doc.keySet.contains("_r") == true) {
        val subfield_value = value_doc.getString("_r", BsonString("")).getValue().trim()
        val is_valid = Fiadmin.is_code_valid(subfield_value, "degree_of_responsibility")
        if (!is_valid) {
          value_doc.remove("_r")
          value_doc.put("_r", BsonString(""))
          values.set(i, value_doc)
          
          logger.warn(s"biblioref.referencesource;$value_v2;v24_r;Not found in FI Admin - $value")
        }
      }
      i += 1
    )
    this.fields.put("corporate_author_collection", values)

  /**
    * Transforms the "individual_author_collection" field for FI-Admin.
    * Simply adds the content of the field v23 as is and issues
    * a warning if the code is not available in FI-Admin's MySQL database
    * for the subfield _r and _p
    */
  def transform_individual_author_collection() : Unit =
    var values_v23 = get_all_values_as_document("23")
    if (values_v23.size > 0) {
      val value_v2 = get_first_value("2")

      /* Dealing with _p subfield and replacing the country name by its country code */
      var i = 0;
      values_v23.toArray.foreach(value =>
        var value_doc = value.asInstanceOf[BsonDocument]
        if (value_doc.keySet.contains("_p") == true) {
          val value_v23_p = value_doc.get("_p").asString.getValue().trim()
          if (value_v23_p != "") {
            val country_code = Fiadmin.get_country_code(value_v23_p)
            if (country_code == null) {
              logger.warn(s"biblioref.referencesource;$value_v2;v23_p;Not found in FI Admin - $value")
            } else {
              value_doc.remove("_p")
              value_doc.put("_p", BsonString(country_code))
              values_v23.set(i, value_doc)
            }
          }
        }

        /* Dealing with _r subfield */
        if (value_doc.keySet.contains("_r") == true) {
          val value_r = value_doc.get("_r").asString.getValue().trim()
          if (value_r != "") {
            val is_valid = Fiadmin.is_code_valid(value_r, "degree_of_responsibility")
            if (!is_valid) {
              logger.warn(s"biblioref.referencesource;$value_v2;v23_r;Not found in FI Admin - $value")
              value_doc.remove("_r")
              value_doc.put("_r", BsonString(""))
              values_v23.set(i, value_doc)
            }
          }
        }
        i += 1
      )
      
      this.fields.put("individual_author_collection", values_v23)
    }

  /**
    * Transforms the "individual_author_monographic" field for FI-Admin.
    * Simply adds the content of the field v16 as is and issues
    * a warning if the code is not available in FI-Admin's MySQL database
    * for the subfield _r and _p
    */
  def transform_individual_author_monographic() : Unit =
    var values_v16 = get_all_values_as_document("16")
    if (values_v16.size > 0) {
      val value_v2 = get_first_value("2")

      /* Dealing with _p subfield and replacing the country name by its country code */
      var i = 0;
      values_v16.toArray.foreach(value =>
        var value_doc = value.asInstanceOf[BsonDocument]
        if (value_doc.keySet.contains("_p") == true) {
          val value_v16_p = value_doc.get("_p").asString.getValue().trim()
          if (value_v16_p != "") {
            val country_code = Fiadmin.get_country_code(value_v16_p)
            if (country_code == null) {
              logger.warn(s"biblioref.referencesource;$value_v2;v16_p;Not found in FI Admin - $value")
            } else {
              value_doc.remove("_p")
              value_doc.put("_p", BsonString(country_code))
              values_v16.set(i, value_doc)
            }
          }
        }

        /* Dealing with _r subfield */
        if (value_doc.keySet.contains("_r") == true) {
          val value_r = value_doc.get("_r").asString.getValue().trim()
          if (value_r != "") {
            val is_valid = Fiadmin.is_code_valid(value_r, "degree_of_responsibility")
            if (!is_valid) {
              logger.warn(s"biblioref.referencesource;$value_v2;v16_r;Not found in FI Admin - $value")
              value_doc.remove("_r")
              value_doc.put("_r", BsonString(""))
              values_v16.set(i, value_doc)
            }
          }
        }
        i += 1
      )
      
      this.fields.put("individual_author_monographic", values_v16)
    }

  override def set_field_as_string(name: String, key: String) : Unit =
    try {
      super.set_field_as_string(name, key)
    } catch {
      case _: Throwable => {
        val value_v2 = get_first_value("2")
        logger.warn(s"biblioref.referencesource;$value_v2;v$key;Invalid value")
      }
    }