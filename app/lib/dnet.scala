package afm.dnet

import afm._
import afm.io._
import afm.model._
import afm.mongo._
import scala.xml.{XML, Elem, Node}
import com.mongodb.casbah.Imports._


class DNetMongoDBAdapter(implicit config: Config) extends Adapter[DBObject] {
  val xmlAdapter = new XmlAdapter

  def toDocument(record: DBObject): Document = new DnetDocument(record.getAs[String]("id").get,
								xmlAdapter.toDocument(toElem(record.getAs[String]("body").get)).asInstanceOf[MapDocument]fieldMap)

  def toElem(xml: String): Elem = {XML.loadString(xml)}
}

class DnetDocument(val dnetIdentifier: String, fieldMap: Map[String, Field[Any]])(implicit config: Config) extends MapDocument(fieldMap) {
  override def identifier = dnetIdentifier
}

class XmlAdapter(implicit config: Config) extends Adapter[Elem] {

  def toDocument(xml: Elem): Document = new MapDocument(Map(
    (for(field <- config.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _) => IntField(1)
       case StringFieldDef(name, _) => StringField(select(xml, name))
       case ListFieldDef(name, _) => ListField(List(StringField(xml.toString)))
     })
   ):_*
  ))

  def select(node: Elem, name: String): String = {
    name.split("_") match {
      case Array(ns, fieldName) => (node \\ fieldName).filter({k: Node => k.prefix == ns}).text
      case Array(fieldName) => (node \\ fieldName).text
    }
  }

}
