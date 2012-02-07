package afm

import com.mongodb.casbah.Imports._


object MongoUtils {
  def get[A](rs: DBObject, name: String)(implicit manifest: Manifest[A]) = rs.getAs[A](name).getOrElse(throw new Exception("not found or wrong type %s".format(name)))

  def toDocument(rs: DBObject): Document = new Document(Map(
    (for(field <- Model.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _) => IntField(get[Int](rs, name))
       case StringFieldDef(name, _) => StringField(get[String](rs, name))
       case ListFieldDef(name, _) => ListField(for(i <- get[BasicDBList](rs, name)) yield StringField(i.asInstanceOf[String]))
     })
   ):_*
  ))
}
