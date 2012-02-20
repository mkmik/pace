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


sealed abstract class FieldDef[A](val name: String, val algo: DistanceAlgo)

case class IntFieldDef(n: String, d: DistanceAlgo) extends FieldDef[Int](n, d)
case class StringFieldDef(n: String, d: DistanceAlgo) extends FieldDef[String](n, d)
case class ListFieldDef(n: String, d: DistanceAlgo) extends FieldDef[List[String]](n, d)


sealed abstract class Field[+A](val value: A)

case class IntField(override val value: Int) extends Field[Int](value)
case class StringField(override val value: String) extends Field[String](value)
case class ListField(override val value: Seq[Field[String]]) extends Field[Seq[Field[String]]](value)


class Document (val fields: Map[String, Field[Any]]) {
  def apply[A](name: String): Option[Field[A]] = {
    fields.get(name) match {
      case Some(res) => Some(res.asInstanceOf[Field[A]])
      case None => None
    }
  }

  override def toString = "Document(%s)".format(fields)

  def identifier = apply[Int]("n").get.value
  def realIdentifier = if (apply[String]("kind").get.value == "unique") identifier else apply[Int]("relatedTo").get.value
}

