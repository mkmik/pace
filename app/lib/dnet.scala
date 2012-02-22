package afm.dnet

import afm._
import afm.io._
import afm.model._
import afm.mongo._
import scala.xml.{Elem, Node}
import com.mongodb.casbah.Imports._


class DNetMongoDBAdapter(implicit config: Config) extends Adapter[DBObject] {
  val xmlAdapter = new XmlAdapter

  def toDocument(record: DBObject): Document = xmlAdapter.toDocument(record.getAs[Elem]("body").get)
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
    val Array(ns, fieldName) = name.split("_")
    (node \\ fieldName).filter({k: Node => k.prefix == ns}).text
  }

}
