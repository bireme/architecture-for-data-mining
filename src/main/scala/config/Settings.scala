package config
import com.typesafe.config._


object Settings:
  val conf_name = sys.env.get("ISIS2MONGO_SETTINGS").getOrElse("cli")
  val config = ConfigFactory.load(conf_name)
  config.checkValid(ConfigFactory.defaultReference(), conf_name)

  def getConf(name: String): String =
    return config.getString(name)