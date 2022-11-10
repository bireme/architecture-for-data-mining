import isis.IsisDB
import mongo.MongoDB


@main def main =    
  val database = IsisDB()
  database.mount_mst()
  val documents = database.parse_data()

  val mongodb = MongoDB()
  mongodb.connect()
  mongodb.set_collection("01_isiscopy")
  mongodb.insert_documents(documents)