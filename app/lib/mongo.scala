package afm

import com.mongodb.casbah.Imports._


class MongoDBCollector(val coll: MongoCollection)(implicit val config: Config) extends Collector {
  import MongoUtils._

  coll.drop()
  coll.ensureIndex(MongoDBObject("d" -> 1))

  def append(dup: Duplicate) = coll += dup.toMongo

  def realDups = (config.source.count(Map("kind" -> "duplicate")).toDouble * shrinkingFactor).toLong
}

object MongoUtils {
  def get[A](rs: DBObject, name: String)(implicit manifest: Manifest[A]) = rs.getAs[A](name)

  def toDocument(rs: DBObject)(implicit config: Config): Document = new Document(Map(
    (for(field <- config.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _) => IntField(get[Int](rs, name).getOrElse(0))
       case StringFieldDef(name, _) => StringField(get[String](rs, name).getOrElse(""))
       case ListFieldDef(name, _) => ListField(for(i <- get[BasicDBList](rs, name).getOrElse(new BasicDBList())) yield StringField(i.asInstanceOf[String]))
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
    def toMongo = MongoDBObject((for((k, v) <- doc.fields.toIterable.toSeq) yield (k, v.toMongo)) :_*)
  }

  implicit def duplicateToMongo(dup: Duplicate): {def toMongo: DBObject} = new {
    def toMongo = MongoDBObject("d" -> dup.d,
                                "left" -> dup.a.toMongo,
                                "right" -> dup.b.toMongo)
  }

}
