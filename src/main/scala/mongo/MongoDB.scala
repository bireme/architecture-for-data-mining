package mongo
import org.mongodb.scala._
import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.mutable.Document
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.collection.JavaConverters._
import config.Settings


class MongoDB():
  var client: MongoClient = _
  var database: MongoDatabase = _
  var collection: MongoCollection[Document] = _

  def connect() =
    val host = Settings.getConf("MONGODB_HOST")
    val port = Settings.getConf("MONGODB_PORT")
    val user = Settings.getConf("MONGODB_USER")
    val pass = Settings.getConf("MONGODB_PASSWORD")
    val db = Settings.getConf("MONGODB_DATABASE")

    client = MongoClient(s"mongodb://$user:$pass@$host:$port")
    database = client.getDatabase(db)

  def set_collection(name: String) =
    collection = database.getCollection(name)

  def drop_collection() =
    val result = collection.drop()
    result.subscribe(
      Void => {},
      (e: Throwable) => {println(s"Error: $e")},
      () => {}
    )
    Await.ready(result.toFuture, 30.seconds)

  def insert_documents(documents: Array[Document]) =
    Await.result(collection.insertMany(documents).toFuture(), 30.seconds)

  def insert_document(document: Document) =
    Await.result(collection.insertOne(document).toFuture(), 30.seconds)

  def close() =
    client.close()