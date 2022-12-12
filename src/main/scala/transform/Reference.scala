package transform
import mysql.Fiadmin
import mongo.LiteratureType
import config.Settings
import java.time._
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson.BsonString
import scribe.Logger


/**
  * Transforms data into the Biblioref.reference model standard
  */
class Reference():
    val logger = Logger("biblioref.reference")

    var doc: Document = _

    var fields: Document = Document(
      "BIREME_reviewed" -> false,
      "status" -> -3,
      "created_by" -> 2
    )
    var new_doc: Document = Document(
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

      this.set_id()
      this.set_pk(pk)
      this.set_created_updated_datetime()

      this.transform_cooperative_center_code()
      this.transform_item_form()
      this.transform_reference_title()
      this.transform_indexed_database()
      this.transform_lilacs_indexed()
      this.transform_interoperability_source()
      this.transform_text_language()
      this.transform_publication_type()
      this.transform_check_tags()
      this.transform_type_of_computer_file()
      this.transform_type_of_cartographic_material()
      this.transform_type_of_journal()
      this.transform_type_of_visual_material()
      this.transform_specific_designation_of_the_material()

      this.set_field_as_string("record_type", "9")
      this.set_field_as_string("internal_note", "61")
      this.set_field_as_string("publication_date", "64")
      this.set_field_as_string("publication_date_normalized", "65")
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
      
      this.set_field_as_document("electronic_address", "8")
      this.set_field_as_document("descriptive_information", "38")
      this.set_field_as_document("non_decs_region", "82")
      this.set_field_as_document("abstract", "83")
      this.set_field_as_document("author_keyword", "85")
      this.set_field_as_document("local_descriptors", "653")

      this.new_doc += ("fields", this.fields)
      return this.new_doc
    
    /**
      * Sets this projects ID
      */
    def set_id() =
      this.new_doc += ("_id", this.doc.get("_id"))

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
    def set_created_updated_datetime() =
      val tz_datetime = ZonedDateTime.now().toString()
      this.fields.put("created_time", tz_datetime)
      this.fields.put("updated_time", tz_datetime)

    /**
      * Sets simple string fields
      *
      * @param name FI-Admin field name
      * @param key ISIS field number
      */
    def set_field_as_string(name: String, key: String) =
      if (this.doc.keySet.contains(key) == true) {
        if (this.doc.get(key).get.isArray()) {
          this.fields.put(name, this.doc.get(key).get)
        } else {
          this.fields.put(name, this.doc.get(key).get.asDocument().getString("text").getValue().trim())
        }
      }

    /**
      * Sets complex fields as JSON
      *
      * @param name FI-Admin field name
      * @param key ISIS field number
      */
    def set_field_as_document(name: String, key: String) =
      if (this.doc.keySet.contains(key) == true) {
        if (this.doc.get(key).get.isArray()) {
          this.fields.put(name, this.doc.get(key).get)
        } else {
          this.fields.put(name, this.doc.get(key).get.asDocument())
        }
      }

    /**
      * Provided an ISIS field number (key), gets its first occurence as string
      *
      * @param key ISIS field number
      * @return field value
      */
    def get_first_value(key: String): String =
      var field_value: String = ""

      if (this.doc.keySet.contains(key) == true) {
        if (this.doc.get(key).get.isArray()) {
          this.doc.get(key).get.asArray().forEach(row =>
            val occ_value = row.asDocument().getString("text", BsonString("")).getValue().trim()
            if (occ_value != "") {
              field_value = occ_value
            }
          )
        } else {
          field_value = this.doc.get(key).get.asDocument().getString("text", BsonString("")).getValue().trim()
        }
      }

      return field_value

    /**
      * Provided an ISIS field number (key), gets all occurences as Array
      *
      * @param key ISIS field number
      * @return field value
      */
    def get_all_values(key: String): List[String] =
      var field_values: List[String] = List()

      if (this.doc.keySet.contains(key) == true) {
        if (this.doc.get(key).get.isArray()) {
          this.doc.get(key).get.asArray().forEach(row =>
            val occ_value = row.asDocument().getString("text", BsonString("")).getValue().trim()
            field_values = field_values :+ occ_value
          )
        } else {
          field_values = field_values :+ this.doc.get(key).get.asDocument().getString("text", BsonString("")).getValue().trim()
        }
      }

      return field_values

    /**
      * Transforms the "literature_type" and "treatment_level" fields for FI-Admin.
      * Simply adds the content of the fields v5 and v6 IF they are valid 
      * according to a MongoDB collection imported from this project in v5_v6.json file
      *
      * @return if v5 and v6 fields are both valid
      */
    def transform_literature_type(): Boolean =
      val value_v2 = get_first_value("2")
      val value_v5 = get_first_value("5")
      val value_v6 = get_first_value("6")

      val is_valid = LiteratureType.is_literature_type_valid(value_v5, value_v6)
      if (is_valid) {
        this.set_field_as_string("literature_type", "5")
        this.set_field_as_string("treatment_level", "6")
      } else {
        logger.warn(s"biblioref.reference;$value_v2;v5 - v6;Invalid v5 and v6 - $value_v5 - $value_v6")
      }

      return is_valid

    /**
      * Transforms the "type_of_computer_file" field for FI-Admin.
      * Simply adds the content of the field v111 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_type_of_computer_file() =
      this.set_field_as_string("type_of_computer_file", "111")

      val values_v111 = get_all_values("111")
      val value_v2 = get_first_value("2")
      values_v111.foreach(value_v111 =>
        val is_valid = Fiadmin.is_code_valid(value_v111, "type_of_computer_file")
        if (!is_valid) {
          logger.warn(s"biblioref.reference;$value_v2;v111;Not found in FI Admin - $value_v111")
        }
      )

    /**
      * Transforms the "type_of_cartographic_material" field for FI-Admin.
      * Simply adds the content of the field v112 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_type_of_cartographic_material() =
      this.set_field_as_string("type_of_cartographic_material", "112")

      val values_v112 = get_all_values("112")
      val value_v2 = get_first_value("2")
      values_v112.foreach(value_v112 =>
        val is_valid = Fiadmin.is_code_valid(value_v112, "type_of_cartographic_material")
        if (!is_valid) {
          logger.warn(s"biblioref.reference;$value_v2;v112;Not found in FI Admin - $value_v112")
        }
      )

    /**
      * Transforms the "type_of_journal" field for FI-Admin.
      * Simply adds the content of the field v113 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_type_of_journal() =
      this.set_field_as_string("type_of_journal", "113")

      val values_v113 = get_all_values("113")
      val value_v2 = get_first_value("2")
      values_v113.foreach(value_v113 =>
        val is_valid = Fiadmin.is_code_valid(value_v113, "type_of_journal")
        if (!is_valid) {
          logger.warn(s"biblioref.reference;$value_v2;v113;Not found in FI Admin - $value_v113")
        }
      )

    /**
      * Transforms the "type_of_visual_material" field for FI-Admin.
      * Simply adds the content of the field v114 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_type_of_visual_material() =
      this.set_field_as_string("type_of_visual_material", "114")

      val values_v114 = get_all_values("114")
      val value_v2 = get_first_value("2")
      values_v114.foreach(value_v114 =>
        val is_valid = Fiadmin.is_code_valid(value_v114, "type_of_visual_material")
        if (!is_valid) {
          logger.warn(s"biblioref.reference;$value_v2;v114;Not found in FI Admin - $value_v114")
        }
      )

    /**
      * Transforms the "specific_designation_of_the_material" field for FI-Admin.
      * Simply adds the content of the field v115 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_specific_designation_of_the_material() =
      this.set_field_as_string("specific_designation_of_the_material", "115")

      val values_v115 = get_all_values("115")
      val value_v2 = get_first_value("2")
      values_v115.foreach(value_v115 =>
        val is_valid = Fiadmin.is_code_valid(value_v115, "specific_designation_of_the_material")
        if (!is_valid) {
          logger.warn(s"biblioref.reference;$value_v2;v115;Not found in FI Admin - $value_v115")
        }
      )

    /**
      * Transforms the "check_tags" field for FI-Admin.
      * Simply adds the content of the field v76 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_check_tags() =
      this.set_field_as_document("check_tags", "76")

      val values_v76 = get_all_values("76")
      val value_v2 = get_first_value("2")
      values_v76.foreach(value_v76 =>
        val is_valid = Fiadmin.is_code_valid(value_v76, "check_tags")
        if (!is_valid) {
          logger.warn(s"biblioref.reference;$value_v2;v76;Not found in FI Admin - $value_v76")
        }
      )

    /**
      * Transforms the "publication_type" field for FI-Admin.
      * Simply adds the content of the field v71 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_publication_type() =
      this.set_field_as_string("publication_type", "71")

      val values_v71 = get_all_values("71")
      val value_v2 = get_first_value("2")
      values_v71.foreach(value_v71 =>
        val is_valid = Fiadmin.is_code_valid(value_v71, "publication_type")
        if (!is_valid) {
          logger.warn(s"biblioref.reference;$value_v2;v71;Not found in FI Admin - $value_v71")
        }
      )

    /**
      * Transforms the "text_language" field for FI-Admin.
      * Simply adds the content of the field v40 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_text_language() =
      this.set_field_as_string("text_language", "40")

      val values_v40 = get_all_values("40")
      val value_v2 = get_first_value("2")
      values_v40.foreach(value_v40 =>
        val is_valid = Fiadmin.is_code_valid(value_v40, "text_language")
        if (!is_valid) {
          logger.warn(s"biblioref.reference;$value_v2;v40;Not found in FI Admin - $value_v40")
        }
      )
    
    /**
      * Transforms the "cooperative_center_code" field for FI-Admin.
      * Simply adds the content of the field v1 as is and issues
      * a warning if the code is not available in FI-Admin's MySQL database
      */
    def transform_cooperative_center_code() =
      this.set_field_as_string("cooperative_center_code", "1")

      val value_v1 = get_first_value("1")
      val is_v1_valid = Fiadmin.is_cc_valid(value_v1)
      if (!is_v1_valid) {
        val value_v2 = get_first_value("2")
        logger.warn(s"biblioref.reference;$value_v2;v1;Not found in FI Admin - $value_v1")
      }

    /**
      * Transforms the "interoperability_source" field for FI-Admin.
      * The format returned here is joins the database ID (v2) and a database code
      * defined in the settings file.
      */
    def transform_interoperability_source() =
      var field_value: Document = null

      val value_v2 = get_first_value("2")
      if (value_v2 == "") {
        logger.warn(s"biblioref.reference;;v2;Not found")
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
    def transform_lilacs_indexed() =
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

      this.fields.put("LILACS_indexed", field_value)

    /**
      * Transforms the "reference_title" field for FI-Admin.
      * The format returned here is similar to the citation form of a paper.
      */
    def transform_reference_title() =
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
    def transform_item_form() =
      // Fetches the field value
      var field_value: String = get_first_value("110").toLowerCase()
      
      // Transforms the field value
      if (field_value == "") {
        if (this.doc.keySet.contains("8") == true) {
          field_value = "s"
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
              field_value = field_value.replace(string_to_replace, code)
            )
          )
      }

      if (field_value != "") {
        this.fields.put("item_form", field_value)
      }

    /**
      * Transforms the "indexed_database" field for FI-Admin.
      * Fetched the Fi-admin code for each database entry 
      * (field v4) in this record.
      */
    def transform_indexed_database() =
      var indexed_databases: List[String] = List()
      val values_v4 = get_all_values("4")
      values_v4.foreach(value_v4 =>
        val code = Fiadmin.get_database_id(value_v4)
        if (code != null) {
          indexed_databases = indexed_databases :+ code
        } else {
          val value_v2 = get_first_value("2")
          logger.warn(s"biblioref.reference;$value_v2;v4;Not found in FI Admin - $value_v4")
        }
      )

      if (indexed_databases.length > 0) {
        this.fields.put("indexed_database", indexed_databases)
      }