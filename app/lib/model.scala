package afm.model

import scala.collection._
import scala.collection.immutable.Map
import com.mongodb.casbah.Imports._
import com.typesafe.config.ConfigValue

import afm._
import afm.mongo._
import afm.distance._
import afm.detectors._
import afm.duplicates._


/*! Our model defines a document which is composed by a list of fields, and an identifier
 */
trait Document {
  def fields: Iterable[(String, Field[Any])]
  def apply[A](name: String): Option[Field[A]]

  def identifier: Any
  def realIdentifier: Any
}

/*!
 The schema is composed by field definitions (FieldDef). Each field has a type,
 a name, and an associated distance algorithm
 */
sealed abstract class FieldDef[A](val name: String, val algo: DistanceAlgo, val ignoreMissing: Boolean) {
  def apply(s: String): Field[A]
}

/*! We currently handle only basic field types (int, string and list of strings) */
case class IntFieldDef(n: String, d: DistanceAlgo, i: Boolean) extends FieldDef[Int](n, d, i) {
  def apply(s: String) = IntField(Integer.parseInt(s))
}

case class StringFieldDef(n: String, d: DistanceAlgo, i: Boolean) extends FieldDef[String](n, d, i) {
  def apply(s: String) = StringField(n)
}

case class ListFieldDef(n: String, d: DistanceAlgo, i: Boolean) extends FieldDef[List[String]](n, d, i) {
  def apply(s: String) = throw new Exception("Not implemented yet")
}


/*! Each concrete document is built of field values */
sealed abstract class Field[+A](val value: A) {
  def isEmpty: Boolean = false
}

/*! of which we have concrete marker implementations */
case class IntField(override val value: Int) extends Field[Int](value)

case class StringField(override val value: String) extends Field[String](value) {
  override def isEmpty = value == null || value == "" || value.endsWith("01-01")
}

case class ListField(override val value: Seq[Field[String]]) extends Field[Seq[Field[String]]](value) {
  override def isEmpty = value == null || value.isEmpty || value.head.isEmpty
}


/*! A concrete implementation of a document is done by keeping a map of string->fields */
class MapDocument (val fieldMap: Map[String, Field[Any]])(implicit config: Config) extends Document {
  def fields = fieldMap.toIterable

  def apply[A](name: String): Option[Field[A]] = {
    fieldMap.get(name) match {
      case Some(res) => Some(res.asInstanceOf[Field[A]])
      case None => None
    }
  }

  override def toString = "Document(%s)".format(fieldMap)

  def identifier = apply[Any](config.identifierField).get.value

  def realIdentifier = apply[String]("kind") match {
    case Some(StringField("unique")) => identifier
    case Some(_) => apply[Any]("relatedTo").get.value
    case None => ""
  }

}

