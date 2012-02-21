package afm.io

import com.mongodb.casbah.Imports._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import afm.model._


trait Source {
  def documents(sortKey: String): Iterator[Document]
  def get[A](id: A): Option[Document]
  def get[A](ids: Seq[A]): Iterator[Document]
  def count: Long
  def count(query: Map[String, Any]): Long
}

trait Adapter[A] {
  def toDocument(record: A): Document
}
