package afm.dnet

import afm._
import afm.io._
import afm.model._
import afm.mongo._
import com.mongodb.casbah.Imports._


class DNetMongoDBAdapter(implicit config: Config) extends Adapter[DBObject] {
  val xmlAdapter = new XmlAdapter

  def toDocument(record: DBObject): Document = xmlAdapter.toDocument(record.getAs[String]("body").get)
}

class XmlAdapter(implicit config: Config) extends Adapter[String] {
  def toDocument(xml: String): Document = throw new Exception("not implemented yet")
}
