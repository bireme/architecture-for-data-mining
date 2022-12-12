package mongo
import config.Settings
import org.mongodb.scala._
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Filters._
import scala.collection.JavaConverters._
import scala.concurrent.*
import scala.concurrent.duration.*


/**
  * An abstraction to handle Literature Type data in MongoDB
  */
object LiteratureType:
  val host = Settings.getConf("MONGODB_HOST")
  val port = Settings.getConf("MONGODB_PORT")
  val user = Settings.getConf("MONGODB_USER")
  val pass = Settings.getConf("MONGODB_PASSWORD")
  val db = Settings.getConf("MONGODB_DATABASE")

  val client = MongoClient(s"mongodb://$user:$pass@$host:$port")
  val database = client.getDatabase(db)
  val collection = database.getCollection("02_domain_v5v6")

  var is_literature_type_valid_cache: Map[String, Boolean] = Map()
  def is_literature_type_valid(value_v5: String, value_v6: String) : Boolean =
    var is_valid = false

    val cache_key = "$value_v5-$value_v6"
    if (this.is_literature_type_valid_cache.contains(cache_key)) {
      is_valid = this.is_literature_type_valid_cache(cache_key)
    } else {
      var docs = this.collection.find(
        and(
          equal("v5", value_v5), 
          equal("v6", value_v6)
        )
      )

      docs.subscribe(
        (doc: Document) => {
          is_valid = true
        },
        (e: Throwable) => {println(s"Error: $e")},
        () => { 
          this.is_literature_type_valid_cache += cache_key -> is_valid
        }
      )
      Await.ready(docs.toFuture, 30.seconds)
    }

    return is_valid