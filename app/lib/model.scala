package afm

import scala.collection._


sealed abstract case class FieldDef (val name: String)

case class StringFieldDef(n: String) extends FieldDef(n)
case class ListFieldDef(n: String) extends FieldDef(n)


sealed abstract class Field

case class StringField (val value: String) extends Field {
  override def toString = "Field(%s)".format(value)
}

case class ListField (val values: List[Field]) extends Field {
  override def toString = "ListField(%s)".format(values)
}



class Document (val fields: Map[String, Field]) {
  override def toString = "Document(%s)".format(fields)
}

/////

object Model {
  val fields = List(StringFieldDef("dc_title"),
                    ListFieldDef("dc_creator"))

}
