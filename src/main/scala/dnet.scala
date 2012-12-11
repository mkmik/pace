package afm.dnet

import afm._
import afm.io._
import afm.model._
import scala.xml.{XML, Elem, Node}

class DnetDocument(val dnetIdentifier: String, fieldMap: Map[String, Field[Any]])(implicit config: Config) extends MapDocument(fieldMap) {
  override def identifier = dnetIdentifier
}

class XmlAdapter(implicit config: Config) extends Adapter[Elem] {

  def toDocument(xml: Elem): Document = new MapDocument(Map(
    (for(field <- config.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _, _) => IntField(1)
       case StringFieldDef(name, _, _) => StringField(select(xml, name))
       case ListFieldDef(name, _, _) => ListField(List(StringField(xml.toString)))
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
