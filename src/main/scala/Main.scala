import isis.IsisDB
import mongo.MongoDB
import transform.Transformer
import scala.concurrent.*
import scala.concurrent.duration.*


@main def main =
  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .replace()

  val database = IsisDB()
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

  Thread.sleep(50000)