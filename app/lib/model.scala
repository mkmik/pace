package afm

import scala.collection._


sealed abstract case class FieldDef (val name: String, algo: DistanceAlgo)

case class StringFieldDef(n: String, d: DistanceAlgo) extends FieldDef(n, d)
case class ListFieldDef(n: String, d: DistanceAlgo) extends FieldDef(n, d)


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
  val fields = List(StringFieldDef("dc_title", JaroWinkler(1.0)),
                    ListFieldDef("dc_creator", JaroWinkler(1.0)))

}
