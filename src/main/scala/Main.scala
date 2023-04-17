import isis.IsisDB
import mongo.MongoDB
import transform.Transformer
import dedup.Deduplication
import exporter.JsonExport


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
  Thread.sleep(10000)
  

  Transformer.transform_docs()

  Deduplication().run()

  JsonExport.run()