package afm

import afm.model._


object JsonUtils {
  def toDocument(rs: Map[String, Any])(implicit config: Config) = new MapDocument(Map(
    (for(field <- config.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _) => IntField(rs.get(name).getOrElse(0.0).asInstanceOf[Double].toInt)
       case StringFieldDef(name, _) => StringField(rs.get(name).getOrElse("").asInstanceOf[String])
       case ListFieldDef(name, _) => throw new Exception("not implemented yet")
     })
   ):_*
  ))
}


object CSVUtils {
  def toDocument(line: Array[String])(implicit config: Config) = {
    val rs = line.iterator

    new MapDocument(Map(
      (for(field <- config.fields)
       yield (field.name, field match {
         case IntFieldDef(name, _) => val n = rs.next; IntField(Integer.parseInt(if(n=="") "0" else n))
         case StringFieldDef(name, _) => StringField(rs.next)
         case ListFieldDef(name, _) => throw new Exception("not implemented yet")
       })
     ):_*
    ))
  }
}
