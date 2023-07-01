package transform
import transform.Base_Reference
import mysql.Fiadmin
import mongo.LiteratureType
import mongo.DescriptiveInfo
import config.Settings
import java.time._
import java.text.SimpleDateFormat
import java.text.ParseException
import java.time.format.DateTimeFormatter
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson.BsonString
import org.bson.BsonDocument
import com.typesafe.scalalogging.Logger


/**
  * Transforms data into the Biblioref.reference model standard
  */
class Reference extends Base_Reference:
    val logger = Logger("default")

    fields = Document(
      "BIREME_reviewed" -> Settings.getConf("BIREME_REVIEWED").toBoolean,
      "status" -> Settings.getConf("STATUS").toInt,
      "created_by" -> Settings.getConf("CREATED_BY").toInt
    )
    new_doc = Document(
      "model" -> "biblioref.reference"
    )

    /**
      * Wrapper to transform all fields into the Biblioref.reference model standard
      *
      * @param doc Document to be transformed
      * @param pk Primary Key for this document
      */
    def transform(doc: Document, pk: Int): Document =
      this.doc = doc
      
      var is_valid = this.transform_literature_type()
      if (!is_valid) {
        return null
      }

      this.set_pk(pk)
      this.set_created_updated_datetime()

      this.transform_cooperative_center_code()
      this.transform_item_form()
      this.transform_reference_title()
      this.transform_indexed_database()
      this.transform_lilacs_indexed()
      this.transform_interoperability_source()
      this.transform_check_tags()
      this.transform_electronic_address()
      this.transform_abstract()
      this.transform_author_keyword()
      this.transform_publication_date_normalized()
      this.transform_descriptive_information()
      
      this.transform_field("type_of_computer_file", "111")
      this.transform_field("type_of_cartographic_material", "112")
      this.transform_field("type_of_journal", "113")
      this.transform_field("type_of_visual_material", "114")
      this.transform_field("specific_designation_of_the_material", "115")
      this.transform_field("record_type", "9")
      this.transform_field("publication_type", "71")
      this.transform_field("text_language", "40")
      
      this.set_field_as_string("internal_note", "61")
      this.set_field_as_string("publication_date", "64")
      this.set_field_as_string("total_number_of_references", "72")
      this.set_field_as_string("time_limits_from", "74")
      this.set_field_as_string("time_limits_to", "75")
      this.set_field_as_string("person_as_subject", "78")
      this.set_field_as_string("transfer_date_to_database", "84")
      this.set_field_as_string("general_note", "500")
      this.set_field_as_string("formatted_contents_note", "505")
      this.set_field_as_string("additional_physical_form_available_note", "530")
      this.set_field_as_string("reproduction_note", "533")
      this.set_field_as_string("original_version_note", "534")
      this.set_field_as_string("institution_as_subject", "610")
      this.set_field_as_string("software_version", "899")
      
      this.set_field_as_document("non_decs_region", "82")
      this.set_field_as_document("local_descriptors", "653")

      this.new_doc += ("fields", this.fields)
      return this.new_doc

    /**
      * Sets BIREME's primary key
      *
      * @param pk
      */
    def set_pk(pk: Int) =
      this.new_doc += ("pk", pk)

    /**
      * Sets the created and updated date and time with
      * timezone
      */
    def set_created_updated_datetime() : Unit =
      val tz_datetime = ZonedDateTime.now()
      val formatted_datetime = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZZZZZ").format(tz_datetime)
      this.fields.put("created_time", formatted_datetime)
      this.fields.put("updated_time", formatted_datetime)

    /**
      * Transforms the "literature_type" and "treatment_level" fields for FI-Admin.
      * Simply adds the content of the fields v5 and v6 IF they are valid 
      * according to a MongoDB collection imported from this project in v5_v6.json file
      *
      * @return if v5 and v6 fields are both valid
      */
    def transform_literature_type() : Boolean =
      val value_v2 = get_first_value("2")
      val value_v5 = get_first_value("5")
      val value_v6 = get_first_value("6")

      val is_valid = LiteratureType.is_literature_type_valid(value_v5, value_v6)
      if (is_valid) {
        this.set_field_as_string("literature_type", "5")
        this.set_field_as_string("treatment_level", "6")
      } else {
        logger.warn(s"5,text,$value_v5")
        logger.warn(s"6,text,$value_v6")
      }

      return is_valid

    /**
      * Generic transformer for a Fi-Admin field.
      * Simply adds the content of the field isis_field as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_field(fiadmin_field: String, isis_field: String) : Unit =
      var values = get_all_values(isis_field)
      val value_v2 = get_first_value("2")
      var i = 0

      if (values.size >= 1) {
        values.foreach(value =>
          if (value.trim() != "") {
            val is_valid = Fiadmin.is_code_valid(value, fiadmin_field)
            if (!is_valid) {
              values = values.updated(i, "")
              logger.warn(s"$isis_field,text,$value")
            }
            i += 1
          }
        )
        this.fields.put(fiadmin_field, values)
      }

    /**
      * Transforms the "descriptive_information" field for FI-Admin.
      * Simply adds the content of the field v38 as is and issues
      * a warning if the code is not available in MongoDB database
      * for the subfield _b
      */
    def transform_descriptive_information() : Unit =
      this.set_field_as_document("descriptive_information", "38")

      val value_v2 = get_first_value("2")
      val values_v38_b = get_all_values("38", "_b")
      values_v38_b.foreach(value_v38_b =>
        val is_valid = DescriptiveInfo.is_descriptive_code_valid(value_v38_b)
        if (!is_valid) {
          logger.warn(s"38,_b,$value_v38_b")
        }
      )

    /**
      * Transforms the "abstract" field for FI-Admin.
      * Simply adds the content of the field v83 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      * for the subfield _i
      */
    def transform_abstract() : Unit =
      val value_v2 = get_first_value("2")
      val values = get_all_values_as_document("83")
      var i = 0

      if (values.size >= 1) {
        values.forEach(value =>
          var value_doc = value.asDocument()
          if (value_doc.keySet.contains("_i") == true) {
            val subfield_value = value_doc.getString("_i", BsonString("")).getValue().trim()
            if (subfield_value != "") {
              val is_valid = Fiadmin.is_code_valid(subfield_value, "text_language")
              if (!is_valid) {
                value_doc.remove("_i")
                value_doc.put("_i", BsonString(""))
                values.set(i, value_doc)
                
                logger.warn(s"83,_i,$subfield_value")
              }
            }
          }
          i += 1
        )
        this.fields.put("abstract", "["+values.toArray.mkString(", ")+"]")
      }

    /**
      * Transforms the "author_keyword" field for FI-Admin.
      * Simply adds the content of the field v85 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      * for the subfield _i
      */
    def transform_author_keyword() : Unit =
      val value_v2 = get_first_value("2")
      val values = get_all_values_as_document("85")
      var i = 0

      if (values.size >= 1) {
        values.forEach(value =>
          var value_doc = value.asDocument()
          if (value_doc.keySet.contains("_i") == true) {
            val subfield_value = value_doc.getString("_i", BsonString("")).getValue().trim()
            if (subfield_value != ""){
              val is_valid = Fiadmin.is_code_valid(subfield_value, "text_language")
              if (!is_valid) {
                value_doc.remove("_i")
                value_doc.put("_i", BsonString(""))
                values.set(i, value_doc)
                
                logger.warn(s"85,_i,$subfield_value")
              }
            }
          }
          i += 1
        )
        this.fields.put("author_keyword", "["+values.toArray.mkString(", ")+"]")
      }

    /**
      * Transforms the "electronic_address" field for FI-Admin.
      * Simply adds the content of the field v8 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      * for the subfields _i, _q and _y
      */
    def transform_electronic_address() : Unit =
      var values_v8 = get_all_values_as_document("8")
      if (values_v8.size > 0) {
        val value_v2 = get_first_value("2")

        var i = 0
        values_v8.toArray.foreach(value =>
          var value_doc = value.asInstanceOf[BsonDocument]

          /* Dealing with _i subfield */
          if (value_doc.keySet.contains("_i") == true) {
            val value_i = value_doc.get("_i").asString.getValue().trim()
            if (value_i != "") {
              val is_valid = Fiadmin.is_code_valid(value_i, "text_language")
              if (!is_valid) {
                logger.warn(s"8,_i,$value_i")
                value_doc.remove("_i")
                value_doc.put("_i", BsonString(""))
                values_v8.set(i, value_doc)
              }
            }
          }

          /* Dealing with _q subfield */
          if (value_doc.keySet.contains("_q") == true) {
            val value_q = value_doc.get("_q").asString.getValue().trim()
            if (value_q != "") {
              val is_valid = Fiadmin.is_code_valid(value_q, "electronic_address_q")
              if (!is_valid) {
                logger.warn(s"8,_q,$value_q")
                value_doc.remove("_q")
                value_doc.put("_q", BsonString(""))
                values_v8.set(i, value_doc)
              }
            }
          }

          /* Dealing with _y subfield */
          if (value_doc.keySet.contains("_y") == true) {
            val value_y = value_doc.get("_y").asString.getValue().trim()
            if (value_y != "") {
              val is_valid = Fiadmin.is_code_valid(value_y, "electronic_address_y")
              if (!is_valid) {
                logger.warn(s"8,_y,$value_y")
                value_doc.remove("_y")
                value_doc.put("_y", BsonString(""))
                values_v8.set(i, value_doc)
              }
            }
          }

          i += 1
        )
        
        this.fields.put("electronic_address", "["+values_v8.toArray.mkString(", ")+"]")
      }

    /**
      * Transforms the "check_tags" field for FI-Admin.
      * Simply adds the content of the field v76 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_check_tags() : Unit =
      val value_v2 = get_first_value("2")
      val values = get_all_values_as_document("76")
      var i = 0

      if (values.size >= 1) {
        values.forEach(value =>
          var value_doc = value.asDocument()
          if (value_doc.keySet.contains("text") == true) {
            val subfield_value = value_doc.getString("text", BsonString("")).getValue().trim()

            if (subfield_value != "") {
              val is_valid = Fiadmin.is_code_valid(subfield_value, "check_tags")
              if (!is_valid) {
                value_doc.remove("text")
                value_doc.put("text", BsonString(""))
                values.set(i, value_doc)
                
                logger.warn(s"76,text,$subfield_value")
              }
            }
          }
          i += 1
        )
        this.fields.put("check_tags", "["+values.toArray.mkString(", ")+"]")
      }
    
    /**
      * Transforms the "publication_date_normalized" field for FI-Admin.
      * Simply adds the content of the field v1 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_publication_date_normalized() : Unit =
      this.set_field_as_string("publication_date_normalized", "65")

      val values_v65 = get_all_values("65")
      var i = 0

      if (values_v65.size >= 1) {
        values_v65.foreach(value_v65 =>
          if (value_v65.length == 8) {
            val format = new SimpleDateFormat("yyyyMMdd")
            try {
              format.parse(value_v65)
            } catch  {
              case e: ParseException => logger.warn(s"65,text,$value_v65")
            }
          } else if (value_v65.length < 8) {
            val padded_v65 = value_v65.padTo(8, '0')
            values_v65.updated(i, padded_v65)
          } else {
            logger.warn(s"65,text,$value_v65")
          }
          i += 1
        )
      }

    /**
      * Transforms the "cooperative_center_code" field for FI-Admin.
      * Simply adds the content of the field v1 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_cooperative_center_code() : Unit =
      this.set_field_as_string("cooperative_center_code", "1")

      val value_v1 = get_first_value("1")
      val is_v1_valid = Fiadmin.is_cc_valid(value_v1)
      if (!is_v1_valid) {
        val value_v2 = get_first_value("2")
        logger.warn(s"1,text,$value_v1")
      }

    /**
      * Transforms the "interoperability_source" field for FI-Admin.
      * The format returned here is joins the database ID (v2) and a database code
      * defined in the settings file.
      */
    def transform_interoperability_source() : Unit =
      var field_value: Document = null

      val value_v2 = get_first_value("2")
      if (value_v2 == "") {
        logger.warn(s"2,text,")
      }

      val interop_source = Settings.getConf("INTEROPERABILITY_SOURCE")
      field_value = Document(
        "text" -> interop_source,
        "_i" -> value_v2
      )

      this.fields.put("interoperability_source", field_value)

    /**
      * Transforms the "lilacs_indexed" field for FI-Admin.
      * This means that the record is indexed in LILACS.
      * If it is a journal, checks the ISSN or the journal title.
      * Otherwise, checks for the database name.
      */
    def transform_lilacs_indexed() : Unit =
      var field_value: Boolean = false

      val value_v5 = get_first_value("5")
      val value_v6 = get_first_value("6")

      if (value_v5 == "s" && value_v6 == "as") {
        val value_v30 = get_first_value("30")
        val value_v35 = get_first_value("35")
        if (value_v35 != "") {
          field_value = Fiadmin.is_issn_lilacs(value_v35)
        } else if (value_v30 != "") {
          field_value = Fiadmin.is_journal_title_lilacs(value_v30)
        }
      } else {
        val values_v4 = get_all_values("4")
        if (values_v4.contains("LILACS")) {
          field_value = true
        }
      }

      if (field_value) {
        val value_v65 = get_first_value("65")
        if (value_v65 != "") {
          val year = value_v65.substring(0, 4).toInt
          if (year < 1985) {
            field_value = false
          }
        } else {
          field_value = false
        }
      }

      this.fields.put("LILACS_indexed", field_value)

    /**
      * Transforms the "reference_title" field for FI-Admin.
      * The format returned here is similar to the citation form of a paper.
      */
    def transform_reference_title() : Unit =
      var field_value: String = ""

      val value_v30 = get_first_value("30")
      if (value_v30 != "") {
        field_value = value_v30
      }

      val value_v6 = get_first_value("6")
      val value_v12 = get_first_value("12")
      val value_v65 = get_first_value("65").slice(0, 4)

      if (value_v6 == "as") {
        val value_v31 = get_first_value("31")
        val value_v32 = get_first_value("32")
        field_value += "; " + value_v31 + " (" + value_v32 + "), " + value_v65
      } else {
        val value_v18 = get_first_value("18")
        val value_v25 = get_first_value("25")

        if (value_v30 != "") {
          field_value += " | "
        }
        if (value_v25 != "") {
          field_value += value_v25 + " | "
        }
        field_value += value_v18 + ", " + value_v65
      }

      if (value_v12 != "") {
        field_value += " | " + value_v12
      }

      if (field_value != "") {
        this.fields.put("reference_title", field_value)
      }

    /**
      * Transforms the "item_form" field for FI-Admin.
      * This field must be registered with an one character code.
      * In some cases, the field is indexed with the extended form.
      * This method aims to replace the extended version for the code when possible.
      * If not, it will check for the eletronic address field and then will place a specific code.
      */
    def transform_item_form() : Unit =
      // Fetches the field value
      val field_value: String = get_first_value("110").toLowerCase()
      var new_field_value: String = ""
      
      // Transforms the field value
      if (field_value == "") {
        if (this.doc.keySet.contains("8") == true) {
          new_field_value = "s"
        }
      } else if (field_value.length > 1) {
          val replacements = Array(
            Array("a",	Array("microfilm",	"microfilme",	"microfilm")),
            Array("b",	Array("microficha",	"microficha",	"microfiche")),
            Array("c",	Array("microficha opaca",	"microficha opaca",	"microopaque")),
            Array("d",	Array("impreso grande",	"impresso grande",	"large print")),
            Array("f",	Array("braille")),
            Array("r",	Array("reproducción impresa regular - impresión legible",	"reprodução impressa regular - impressão legível",	"regular print reproduction - eye-readable print")),
            Array("s",	Array("electrónico",	"eletrônico",	"electronic")),
            Array("|",	Array("no se codifica",	"não se codifica",	"no attempt to code"))
          )

          replacements.foreach(replacement =>
            val code = replacement(0).asInstanceOf[String]
            val strings_to_replace = replacement(1).asInstanceOf[Array[String]]
            strings_to_replace.foreach(string_to_replace =>
              new_field_value = field_value.replace(string_to_replace, code)
            )
          )
      }

      if (new_field_value != "") {
        this.fields.put("item_form", new_field_value)
      }

    /**
      * Transforms the "indexed_database" field for FI-Admin.
      * Fetched the Fi-admin code for each database entry 
      * (field v4) in this record.
      */
    def transform_indexed_database() : Unit =
      var indexed_databases: List[String] = List()
      val values_v4 = get_all_values("4")
      values_v4.foreach(value_v4 =>
        val code = Fiadmin.get_database_id(value_v4)
        if (code != null) {
          indexed_databases = indexed_databases :+ code
        }
      )

      if (indexed_databases.length > 0) {
        this.fields.put("indexed_database", indexed_databases)
      }