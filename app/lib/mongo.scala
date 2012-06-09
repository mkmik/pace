package afm.mongo

import afm._
import afm.io._
import afm.model._
import afm.duplicates._
import com.mongodb.casbah.Imports._


class MongoDBSource(val collection: MongoCollection, val adapter: Adapter[DBObject])(implicit config: Config) extends Source {
  def documents(sortKey: String) = collection.find().sort(Map(sortKey -> 1)) map adapter.toDocument
  def get[A](id: A) = collection.findOne(Map(config.identifierField -> id)) map adapter.toDocument
  def get[A](ids: Seq[A]) = collection.find(config.identifierField $in ids) map adapter.toDocument

  def count = collection.count
  def count(query: Map[String, Any]) = collection.count(query)
}

class BSONAdapter(implicit config: Config) extends Adapter[DBObject] {
  def toDocument(record: DBObject): Document = MongoUtils.toDocument(record)
}

class MongoDBCollector(val coll: MongoCollection)(implicit val config: Config) extends Collector {
  import MongoUtils._

  coll.drop()
  coll.ensureIndex(MongoDBObject("d" -> 1))

  val rejectedColl = config.mongoDb("rejected")
  rejectedColl.drop()
  rejectedColl.ensureIndex(MongoDBObject("d" -> 1))

  def append(dup: Duplicate) = {
    val c = dup match {
      case Duplicate(_, _, _, false) => coll
      case Duplicate(_, _, _, true) => rejectedColl
    }
    c += dup.toMongo
  }

  def realDups = (config.source.count(Map("kind" -> "duplicate")).toDouble * shrinkingFactor).toLong
}

object MongoUtils {
  def get[A](rs: DBObject, name: String)(implicit manifest: Manifest[A]) = rs.getAs[A](name)

  def toDocument(rs: DBObject)(implicit config: Config): Document = new MapDocument(Map(
    (for(field <- config.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _, _) => IntField(get[Int](rs, name).getOrElse(0))
       case StringFieldDef(name, _, _) => StringField(get[String](rs, name).getOrElse(""))
       case ListFieldDef(name, _, _) => ListField(for(i <- get[BasicDBList](rs, name).getOrElse(new BasicDBList())) yield StringField(i.asInstanceOf[String]))
     })
   ):_*
  ))


  implicit def fieldToMongo(field: Field[Any]): {def toMongo: Any} = new {
    def toMongo = field match {
      case IntField(value) => value
      case StringField(value) => value
      case ListField(values) => values
    }
  }

  implicit def documentToMongo(doc: Document): {def toMongo: DBObject} = new {
    def toMongo = MongoDBObject((for((k, v) <- doc.fields.toSeq) yield (k, v.toMongo)) :_*)
  }

  implicit def duplicateToMongo(dup: Duplicate): {def toMongo: DBObject} = new {
    def toMongo = MongoDBObject("d" -> dup.d,
                                "left" -> dup.a.toMongo,
                                "right" -> dup.b.toMongo)
  }

}
