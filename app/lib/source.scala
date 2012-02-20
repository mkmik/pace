package afm

import com.mongodb.casbah.Imports._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


trait Source {
  def documents(sortKey: String): Iterator[Document]
  def get[A](id: A): Option[Document]
  def get[A](ids: Seq[A]): Iterator[Document]
  def count: Long
  def count(query: Map[String, Any]): Long
}

class MongoDBSource(val collection: MongoCollection)(implicit config: Config) extends Source {
  def documents(sortKey: String) = collection.find().sort(Map(sortKey -> 1)) map MongoUtils.toDocument
  def get[A](id: A) = collection.findOne(Map("n" -> id)) map MongoUtils.toDocument
  def get[A](ids: Seq[A]) = collection.find("n" $in ids) map MongoUtils.toDocument
  def count = collection.count
  def count(query: Map[String, Any]) = collection.count(query)
}
