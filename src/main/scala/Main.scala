import isis.IsisDB
import mongo.MongoDB
import transform.Transformer
//import org.bireme.dcdup.DoubleCheckDuplicated


@main def main =
  scribe.Logger.root
    .clearHandlers()
    .clearModifiers()
    .replace()

  /*
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
  */

  Transformer.transform_docs()
  /*DoubleCheckDuplicated.doubleCheck(
    "/home/ubuntu/architecture-for-data-mining/temp/in.pipe",
    "UTF-8",
    "/home/ubuntu/architecture-for-data-mining/data/Dedup/indexes/lilacs_Sas",
    "/home/ubuntu/architecture-for-data-mining/data/Dedup/configs/configLILACS_Sas_Seven.cfg",
    "UTF-8",
    "/home/ubuntu/architecture-for-data-mining/temp/out1",
    "/home/ubuntu/architecture-for-data-mining/temp/out2",
    "/home/ubuntu/architecture-for-data-mining/temp/outno1",
    "/home/ubuntu/architecture-for-data-mining/temp/outno2"
  )*/

  Thread.sleep(50000)