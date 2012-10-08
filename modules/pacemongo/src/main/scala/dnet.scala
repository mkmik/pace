package afm.mongo.dnet

import afm._
import afm.io._
import afm.model._
import afm.dnet._
import afm.mongo._
import scala.xml.{XML, Elem, Node}
import com.mongodb.casbah.Imports._


class DNetMongoDBAdapter(implicit config: Config) extends Adapter[DBObject] {
  val xmlAdapter = new XmlAdapter

  def toDocument(record: DBObject): Document = new DnetDocument(record.getAs[String]("id").get,
								xmlAdapter.toDocument(toElem(record.getAs[String]("body").get)).asInstanceOf[MapDocument]fieldMap)

  def toElem(xml: String): Elem = {XML.loadString(xml)}
}
