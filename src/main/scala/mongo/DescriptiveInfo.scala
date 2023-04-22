package mongo
import config.Settings
import org.mongodb.scala._
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Filters._
import scala.collection.JavaConverters._
import scala.concurrent.*
import scala.concurrent.duration.*


/**
  * An abstraction to handle Descriptive Information data in MongoDB
  */
object DescriptiveInfo:
  val host = Settings.getConf("MONGODB_HOST")
  val port = Settings.getConf("MONGODB_PORT")
  val user = Settings.getConf("MONGODB_USER")
  val pass = Settings.getConf("MONGODB_PASSWORD")
  val db = Settings.getConf("MONGODB_DATABASE")

  val client = MongoClient(s"mongodb://$user:$pass@$host:$port")
  val database = client.getDatabase(db)
  val collection = database.getCollection("02_domain_v38")

  var is_descriptive_code_valid_cache: Map[String, Boolean] = Map()
  def is_descriptive_code_valid(value_v38_b: String) : Boolean =
    var is_valid = false

    val cache_key = s"$value_v38_b"
    if (is_descriptive_code_valid_cache.contains(cache_key)) {
      is_valid = is_descriptive_code_valid_cache(cache_key)
    } else {
      var docs = collection.find(
        and(
          equal("_b", value_v38_b)
        )
      )

      docs.subscribe(
        (doc: Document) => {
          is_valid = true
        },
        (e: Throwable) => {println(s"Error: $e")},
        () => { 
          is_descriptive_code_valid_cache += cache_key -> is_valid
        }
      )
      Await.ready(docs.toFuture, 30.seconds)
    }

    return is_valid