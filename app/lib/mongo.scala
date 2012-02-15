package afm

import com.mongodb.casbah.Imports._


object MongoUtils {
  def get[A](rs: DBObject, name: String)(implicit manifest: Manifest[A]) = rs.getAs[A](name)

  def toDocument(rs: DBObject): Document = new Document(Map(
    (for(field <- Model.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _) => IntField(get[Int](rs, name).getOrElse(0))
       case StringFieldDef(name, _) => StringField(get[String](rs, name).getOrElse(""))
       case ListFieldDef(name, _) => ListField(for(i <- get[BasicDBList](rs, name).getOrElse(new BasicDBList())) yield StringField(i.asInstanceOf[String]))
     })
   ):_*
  ))
}


object JsonUtils {
  def toDocument(rs: Map[String, Any]) = new Document(Map(
    (for(field <- Model.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _) => IntField(rs.get(name).getOrElse(0.0).asInstanceOf[Double].toInt)
       case StringFieldDef(name, _) => StringField(rs.get(name).getOrElse("").asInstanceOf[String])
       case ListFieldDef(name, _) => throw new Exception("not implemented yet")
     })
   ):_*
  ))
}
