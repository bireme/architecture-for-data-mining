package isis
import config.Settings

import java.nio.file.Paths
import java.nio.file.Files
import java.io.FileNotFoundException
import java.lang.RuntimeException
import scala.sys.process._
import scala.util.matching.Regex
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonObjectId
import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.mutable.Document
import scala.collection.JavaConverters._


/**
  * Handles the ISIS database, from mounting an ISO file to exporting a dict 
  * to be imported into MongoDB
  *
  * @param iso_path file location for the ISO file containing the ISIS data
  */
class IsisDB(var iso_path: String = ""):
  if (iso_path == "") {
    iso_path = Settings.getConf("ISO_PATH")
  }
  val project_path = Paths.get(".").toAbsolutePath
  val isis_utils_path = s"$project_path/utils/isis1660"
  val mx_path = s"$isis_utils_path/mx"
  val mxcp_path = s"$isis_utils_path/mxcp"
  val retag_path = s"$isis_utils_path/retag"
  val isis_db_path = s"$project_path/data/isis"

  override def toString: String = iso_path

  /**
    * Validates if the ISIS file exists. 
    * Checks if the ISIS utils worked.
    *
    * @param path File path to be validated
    * @return Boolean whether the ISIS file exists
    */
  def validate_file(path: String): Boolean =
    val file_exists = Files.exists(Paths.get(path))
    return file_exists

  /**
    * Cleans the MST ISIS database with mxcp and retag utils
    */
  def clean_mst() =
    val mxcp_cmd = s"$mxcp_path /tmp/isisdb create=/tmp/isisdb_clean clean"
    mxcp_cmd.!!

    val retag_cmd = s"$retag_path /tmp/isisdb_clean unlock"
    retag_cmd.!!

    if (!validate_file(s"/tmp/isisdb_clean.mst")) {
      throw FileNotFoundException(s"An exception occurred while trying to clean the MST file /tmp/isisdb_clean")
    }

  /**
    * Mounts an ISIS database based on an ISO file
    */
  def mount_mst() =
    val mx_cmd = s"$mx_path iso=$iso_path create=/tmp/isisdb -all now"
    try {
      mx_cmd.!!
    } catch {
      case e: RuntimeException => throw RuntimeException(s"An exception occurred while trying to run the MX util to create the ISO file $iso_path: $mx_cmd")
    }

    if (!validate_file(s"/tmp/isisdb.mst")) {
      throw FileNotFoundException(s"An exception occurred while trying to mount the ISO file $iso_path")
    }

    clean_mst()

  /**
    * Parses the field data, splitting the root content from each subfield
    * It then adds to a Document element to be inserted into mongodb
    *
    * @param content the field text string
    */
  def parse_field(content: String): Document =
    val doc = Document()
    
    var root_text = ""
    try {
      root_text = content.split("\\^")(0).trim
    } catch {
      case e: ArrayIndexOutOfBoundsException => {
        root_text = ""
      }
    }
    if (root_text != "") {
      doc.put("text", root_text)
    }

    val keyValPattern: Regex = "(\\^.)([^\\^]+)".r
    for patternMatch <- keyValPattern.findAllMatchIn(content) do
      val sub_field = patternMatch.group(1).replace("^", "_")
      val sub_field_content = patternMatch.group(2)
      doc.put(sub_field, sub_field_content)
    
    return doc

  /**
    * Parses the data inside the ISIS database to Scala Dictionaries
    * where the field number is the key
    */
  def parse_data(): Array[Document] =
    val mx_cmd = s"$mx_path /tmp/isisdb_clean lw=9999999999 now"
    var mx_output = ""
    try {
      mx_output = mx_cmd.!!
    } catch {
      case e: RuntimeException => throw RuntimeException(s"An exception occurred while trying to run the MX util to read the MST file /tmp/isisdb_clean")
    }

    var documents: Array[Document] = Array()
    if (mx_output != "") {
      val field_pattern: Regex = "([0-9]+)  \"(.+)\"".r

      var records: Array[String] = mx_output.split("(?ms)^mfn= ", 0)
      records = records.drop(1) // removing MFN line 
      for record <- records do
        var doc = Document("_id" -> new ObjectId())

        for line <- field_pattern.findAllMatchIn(record.trim()) do
          val field_number = line.group(1)
          var content = this.parse_field(line.group(2))
          
          if (doc.contains(field_number)) {
            if (doc.get(field_number).get.isArray()) {
              var new_value = doc.get(field_number).get.asArray()
              val new_content = BsonArray(content).get(0)
              new_value.add(new_content)

              doc.remove(field_number)
              doc.put(field_number, new_value)
            } else {
              var new_value = BsonArray(doc.get(field_number).get, content)

              doc.remove(field_number)
              doc.put(field_number, new_value)
            }
          } else {
            doc += (field_number, content)
          }
          
        documents = documents :+ doc
    }
    return documents

end IsisDB