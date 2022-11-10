package mongo
import org.mongodb.scala._
import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.mutable.Document
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.collection.JavaConverters._
import config.Settings


class MongoDB():
  val user = "root"
  val passwd = "root"
  var client: MongoClient = _
  var database: MongoDatabase = _
  var collection: MongoCollection[Document] = _

  def connect() =
    val host = Settings.getConf("MONGODB_HOST")

    client = MongoClient(s"mongodb://root:root@$host:27017")
    database = client.getDatabase("isismongodb")

  def set_collection(name: String) =
    collection = database.getCollection(name)

  def insert_documents(documents: Array[Document]) =
    Await.result(collection.insertMany(documents).toFuture(), 10.seconds)

    //collection.find().subscribe((doc: Document) => println(doc.toJson()))