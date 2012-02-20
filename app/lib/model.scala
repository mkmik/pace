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

  def identifier: Int
  def realIdentifier: Int
}

/*!
 The schema is composed by field definitions (FieldDef). Each field has a type,
 a name, and an associated distance algorithm
 */
sealed abstract class FieldDef[A](val name: String, val algo: DistanceAlgo)

/*! We currently handle only basic field types (int, string and list of strings) */
case class IntFieldDef(n: String, d: DistanceAlgo) extends FieldDef[Int](n, d)
case class StringFieldDef(n: String, d: DistanceAlgo) extends FieldDef[String](n, d)
case class ListFieldDef(n: String, d: DistanceAlgo) extends FieldDef[List[String]](n, d)

/*! Each concrete document is built of field values */
sealed abstract class Field[+A](val value: A)

/*! of which we have concrete marker implementations */
case class IntField(override val value: Int) extends Field[Int](value)
case class StringField(override val value: String) extends Field[String](value)
case class ListField(override val value: Seq[Field[String]]) extends Field[Seq[Field[String]]](value)


/*! A concrete implementation of a document is done by keeping a map of string->fields */
class MapDocument (val fieldMap: Map[String, Field[Any]]) extends Document {
  def fields = fieldMap.toIterable

  def apply[A](name: String): Option[Field[A]] = {
    fieldMap.get(name) match {
      case Some(res) => Some(res.asInstanceOf[Field[A]])
      case None => None
    }
  }

  override def toString = "Document(%s)".format(fieldMap)

  def identifier = apply[Int]("n").get.value
  def realIdentifier = if (apply[String]("kind").get.value == "unique") identifier else apply[Int]("relatedTo").get.value
}

