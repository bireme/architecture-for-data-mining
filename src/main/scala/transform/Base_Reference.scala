package transform
import mysql.Fiadmin
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson._
import scala.collection.JavaConverters._
import org.mongodb.scala.ObservableFuture


/**
  * Base class for as Reference transformers
  */
class Base_Reference():
  var doc: Document = _
  var fields: Document = _
  var new_doc: Document = _

  /**
    * Sets simple string fields
    *
    * @param name FI-Admin field name
    * @param key ISIS field number
    */
  def set_field_as_string(name: String, key: String) : Unit =
    if (this.doc.keySet.contains(key) == true) {
      if (this.doc.get(key).get.isArray()) {
        this.fields.put(name, this.doc.get(key).get)
      } else {
        if (this.doc.get(key).get.asDocument().keySet.contains("text") == true) {
          this.fields.put(name, this.doc.get(key).get.asDocument().getString("text").getValue().trim())
        }
      }
    }

  /**
    * Sets complex fields as JSON
    *
    * @param name FI-Admin field name
    * @param key ISIS field number
    */
  def set_field_as_document(name: String, key: String) : Unit =
    if (this.doc.keySet.contains(key) == true) {
      if (this.doc.get(key).get.isArray()) {
        this.fields.put(name, "["+this.doc.get(key).get.asArray.toArray.mkString(", ")+"]")
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
  def get_first_value(key: String, subfield: String = "text"): String =
    var field_value: String = ""

    if (this.doc.keySet.contains(key) == true) {
      if (this.doc.get(key).get.isArray()) {
        this.doc.get(key).get.asArray().forEach(row =>
          val occ_value = row.asDocument().getString(subfield, BsonString("")).getValue().trim()
          if (occ_value != "") {
            field_value = occ_value
          }
        )
      } else {
        field_value = this.doc.get(key).get.asDocument().getString(subfield, BsonString("")).getValue().trim()
      }
    }

    return field_value

  /**
    * Provided an ISIS field number (key), gets all occurences as Array
    *
    * @param key ISIS field number
    * @return field value
    */
  def get_all_values(key: String, subfield: String = "text"): List[String] =
    var field_values: List[String] = List()

    if (this.doc.keySet.contains(key) == true) {
      if (this.doc.get(key).get.isArray()) {
        this.doc.get(key).get.asArray().forEach(row =>
          val occ_value = row.asDocument().getString(subfield, BsonString("")).getValue().trim()
          if (occ_value != "") {
            field_values = field_values :+ occ_value
          }
        )
      } else {
        val occ_value = this.doc.get(key).get.asDocument().getString(subfield, BsonString("")).getValue().trim()
        if (occ_value != "") {
          field_values = field_values :+ occ_value
        }
      }
    }

    return field_values

  /**
    * Provided an ISIS field number (key), gets all occurences as Array of Documents
    *
    * @param key ISIS field number
    * @return field value
    */
  def get_all_values_as_document(key: String): BsonArray =
    if (this.doc.keySet.contains(key) == true) {
      if (this.doc.get(key).get.isArray()) {
        return this.doc.get(key).get.asArray
      } else {
        var new_value = BsonArray()
        new_value.add(this.doc.get(key).get.asDocument)
        return new_value
      }
    }
    return BsonArray()