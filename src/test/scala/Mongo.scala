import isis.IsisDB
import mongo.MongoDB
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Filters._


class Mongo extends munit.FunSuite {
  test("test mongodb documents insert") {
    val database = IsisDB("./src/test/data/testdb.iso")
    database.mount_mst()
    val documents = database.parse_data()

    val mongodb = MongoDB()
    mongodb.connect()
    mongodb.set_collection("x_unittest_case01")
    mongodb.collection.drop().subscribe(x => (), error => (), () => {})

    mongodb.insert_documents(documents)

    val size = mongodb.collection.countDocuments.subscribe(
      (size: Long) => assertEquals(size.toInt, 10)
    )
    
    mongodb.collection.find(equal("2", Document("content" -> "5"))).first().subscribe(
      (doc: org.mongodb.scala.bson.collection.mutable.Document) => {
        assertEquals(doc.get("18").get.asDocument().getString("content").getValue(), "Housing survey for disaster relief and preparedness: Latin America")
        assertEquals(doc.get("18").get.asDocument().getString("_i").getValue(), "en")
      },
      (e: Throwable) => assert(1 == 0),
      () => {}
    )

    mongodb.collection.drop().subscribe(x => (), error => (), () => {})
  }
}
