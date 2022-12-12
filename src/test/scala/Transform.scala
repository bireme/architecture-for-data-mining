import isis.IsisDB
import mongo.MongoDB
import transform.Transformer
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Filters._


class TransformTest extends munit.FunSuite {
  test("test document transformation") {
    val database = IsisDB("./src/test/data/testdb.iso")
    database.mount_mst()
    val documents = database.parse_data()

    val mongodb = MongoDB()
    mongodb.connect()
    mongodb.set_collection("01_isiscopy")
    mongodb.drop_collection()
    mongodb.set_collection("01_isiscopy")
    mongodb.insert_documents(documents)

    var transformer = Transformer()
    transformer.create_biblioref_reference()

    val mongodb_reference = MongoDB()
    mongodb_reference.connect()
    mongodb_reference.set_collection("02_biblioref.reference")

    val size = mongodb_reference.collection.countDocuments.subscribe(
      (size: Long) => assertEquals(size.toInt, 10)
    )
    
    mongodb_reference.collection.find().first().subscribe(
      (doc: org.mongodb.scala.bson.collection.mutable.Document) => {
        val fields = doc.get("fields").get.asDocument()
        assertEquals(fields.getString("reference_title").getValue(), "Vertebral subluxation in chiropractic practice, 1998")
        assertEquals(fields.getString("cooperative_center_code").getValue(), "CL27.1")
        assertEquals(fields.getString("literature_type").getValue(), "M")
        assertEquals(fields.getString("treatment_level").getValue(), "m")
        assertEquals(fields.getString("record_type").getValue(), "a")
        assertEquals(fields.getString("text_language").getValue(), "En")
        assertEquals(fields.getString("publication_date").getValue(), "1998")
        assertEquals(fields.getString("publication_date_normalized").getValue(), "19980000")
        assertEquals(fields.getString("transfer_date_to_database").getValue(), "2009-08-22")
        assertEquals(fields.getBoolean("BIREME_reviewed").getValue(), false)
        assertEquals(fields.getInt32("status").getValue(), -3)
        assertEquals(fields.getInt32("created_by").getValue(), 2)
        assertEquals(doc.get("model").get.asString.getValue(), "biblioref.reference")
      },
      (e: Throwable) => assert(1 == 0),
      () => {}
    )

    mongodb_reference.drop_collection()
  }
}
