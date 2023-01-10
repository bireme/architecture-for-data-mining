package transform
import transform.Base_Reference
import mysql.Fiadmin
import org.mongodb.scala.bson.collection.mutable.Document
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
      * Wrapper to transform all fields into the Biblioref.referenceanalytic model standard
      *
      * @param doc Document to be transformed
      * @param pk Primary Key for this document
      */
    def transform(doc: Document, pk: Int): Document =
      this.doc = doc
      val value_v6 = get_first_value("6")
      if (value_v6.contains("a") == false) {
        this.set_pk(pk)

        this.set_field_as_string("pages_monographic", "20")
        this.set_field_as_string("volume_monographic", "21")
        this.set_field_as_string("english_title_collection", "26")
        this.set_field_as_string("total_number_of_volumes", "27")
        this.set_field_as_string("publisher", "62")
        this.set_field_as_string("edition", "63")
        this.set_field_as_string("publication_city", "66")
        this.set_field_as_string("symbol", "68")
        this.set_field_as_string("isbn", "69")

        this.set_field_as_document("corporate_author_monographic", "17")
        this.set_field_as_document("corporate_author_collection", "24")

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
    def set_pk(pk: Int) =
      this.new_doc += ("pk", pk)

    override def set_field_as_string(name: String, key: String) =
      try {
        super.set_field_as_string(name, key)
      } catch {
        case _: Throwable => {
          val value_v2 = get_first_value("2")
          logger.warn(s"biblioref.referencesource;$value_v2;v65;Invalid v20")
        }
      }