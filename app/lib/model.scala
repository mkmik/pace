package afm

import scala.collection._
import scala.collection.immutable.Map
import com.mongodb.casbah.Imports._


sealed abstract case class FieldDef (val name: String, algo: DistanceAlgo)

case class IntFieldDef(n: String, d: DistanceAlgo) extends FieldDef(n, d)
case class StringFieldDef(n: String, d: DistanceAlgo) extends FieldDef(n, d)
case class ListFieldDef(n: String, d: DistanceAlgo) extends FieldDef(n, d)


sealed abstract class Field {
  def toMongo: Any
}

case class IntField (val value: Int) extends Field {
  override def toString = "Field(%s)".format(value)
  def toMongo = value
}

case class StringField (val value: String) extends Field {
  override def toString = "Field(%s)".format(value)
  def toMongo = value
}

case class ListField (val values: Seq[Field]) extends Field {
  override def toString = "ListField(%s)".format(values)
  def toMongo = values
}



class Document (val fields: Map[String, Field]) {
  override def toString = "Document(%s)".format(fields)

  def identifier = fields("n")
  def realIdentifier = if (fields("kind").asInstanceOf[StringField].value == "unique") identifier else fields("relatedTo")

  def toMongo = MongoDBObject((for((k, v) <- fields.toIterable.toSeq) yield (k, v.toMongo)) :_*)
}

/////

/*! Configuration
 */
trait Config {
  val windowSize = 50
  val threshold = 0.90

  val mongoDb = MongoConnection()("pace")
  val sortOn = "n"
}

trait PaperModel {
  val fields = List(
    IntFieldDef("n", NullDistanceAlgo()),
    StringFieldDef("firstName", JaroWinkler(1.0)),
    StringFieldDef("lastName", JaroWinkler(1.0)),
    StringFieldDef("country", JaroWinkler(1.0)),
    StringFieldDef("birthDate", JaroWinkler(1.0)),
    ListFieldDef("context", JaroWinkler(0.0)),
    StringFieldDef("kind", NullDistanceAlgo()),
    IntFieldDef("relatedTo", NullDistanceAlgo())
  )
}


trait OpenAireModel {
  val fields = List(
    StringFieldDef("dri_objidentifier", NullDistanceAlgo()),
    StringFieldDef("dc_title", JaroWinkler(1.0)),
    StringFieldDef("dc_language", JaroWinkler(1.0)),
    ListFieldDef("dc_creator", JaroWinkler(1.0)) ,
    StringFieldDef("dc_description", JaroWinkler(0.0)),
    StringFieldDef("oaf_affiliationname", JaroWinkler(0))
  )
}

//object Model extends Config with OpenAireModel
object Model extends Config with PaperModel
