package afm

import com.typesafe.config._


object OptionalConfigFactory {
  def load(fileName: String) = new OptionalConfig(ConfigFactory.load(fileName))
}


class OptionalConfig(val conf: com.typesafe.config.Config) {

  def safe[A](getter: String => A)(implicit path: String) = if (conf.hasPath(path)) Some(getter(path)) else None

  def getInt(implicit path: String) = safe(conf.getInt)
  def getString(implicit path: String) = safe(conf.getString)
  def getDouble(implicit path: String) = safe(conf.getDouble)
}
