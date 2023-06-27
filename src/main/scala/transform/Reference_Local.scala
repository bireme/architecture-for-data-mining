package transform
import transform.Base_Reference
import mysql.Fiadmin
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson._


/**
  * Transforms data into the Biblioref.referencelocal model standard
  */
class Reference_Local extends Base_Reference:
  fields = Document()
  new_doc = Document(
    "model" -> "biblioref.referencelocal",
    "pk" -> BsonNull()
  )

  /**
    * Wrapper to transform all fields into the Biblioref.referencelocal model standard
    *
    * @param doc Document to be transformed
    * @param pk Primary Key for this document
    */
  def transform(doc: Document, pk: Int): Document =
    this.doc = doc

    this.set_source(pk)

    this.set_field_as_string("cooperative_center_code", "1")
    this.set_field_as_string("database", "4")
    this.set_field_as_string("internal_note", "61")

    this.set_field_as_document("call_number", "3")
    this.set_field_as_document("inventory_number", "7")
    this.set_field_as_document("local_descriptors", "653")

    this.new_doc += ("fields", this.fields)
    return this.new_doc

  /**
    * Sets BIREME's primary key
    *
    * @param pk
    */
  def set_source(pk: Int) : Unit =
    this.fields.put("source", pk)