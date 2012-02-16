package afm

import scala.collection._
import scala.collection.immutable.Map
import com.mongodb.casbah.Imports._


sealed abstract case class FieldDef[A](val name: String, algo: DistanceAlgo)

case class IntFieldDef(n: String, d: DistanceAlgo) extends FieldDef[Int](n, d)
case class StringFieldDef(n: String, d: DistanceAlgo) extends FieldDef[String](n, d)
case class ListFieldDef(n: String, d: DistanceAlgo) extends FieldDef[List[String]](n, d)


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
  def cores: Option[Int] = None
  def limit: Option[Int] = None
  def windowSize = 10
  def threshold = 0.90

  val mongoDb = MongoConnection()("pace")
  def sortOn = "n"

  def ngramSize = 3
  def maxNgrams = 4

  def simhashRotationStep = 2

  def algo = "singleField"
}

trait OverrideConfig extends Config {
  val conf = OptionalConfigFactory.load("conf/pace.conf")

  override def cores = conf.getInt("pace.cores")
  override def limit = conf.getInt("pace.limit")
  override def windowSize = conf.getInt("pace.windowSize").getOrElse(super.windowSize)
  override def threshold = conf.getDouble("pace.threshold").getOrElse(super.threshold)

  override def sortOn = conf.getString("pace.sortOn").getOrElse(super.sortOn)

  override def ngramSize = conf.getInt("pace.ngramSize").getOrElse(super.ngramSize)
  override def maxNgrams = conf.getInt("pace.maxNgrams").getOrElse(super.maxNgrams)

  override def simhashRotationStep = conf.getInt("pace.simhashRotationStep").getOrElse(super.simhashRotationStep)

  override def algo = conf.getString("pace.algo").getOrElse(super.algo)
}

trait PaperModel {
  val fields = List(
    IntFieldDef("n", NullDistanceAlgo()),
    StringFieldDef("firstName", JaroWinkler(1.0)),
    StringFieldDef("lastName", JaroWinkler(1.0)),
    StringFieldDef("country", JaroWinkler(1.0)),
    StringFieldDef("birthDate", JaroWinkler(1.0)),
//    ListFieldDef("context", JaroWinkler(0.0)),
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
object Model extends OverrideConfig with PaperModel
