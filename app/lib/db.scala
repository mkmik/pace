package afm

import com.twitter.querulous.evaluator.QueryEvaluator
import java.sql.ResultSet

import afm.model._


//val rs = queryEvaluator.select("select * from results_view") { row => row.getArray("dc_creator").getArray.asInstanceOf[Array[String]](0)}.first.split("§§§")(1)

object DbUtils {

  def sqlArrayToList(array: java.sql.Array) = 
    for(i <- (array.getArray.asInstanceOf[Array[String]]).toList)
      yield (if (i != null)  {
        val components = i.split("§§§")
        if(components.length > 1) {
          components(1)
        } else {
          ""
        }
      } else { ""
            })

  def getList(rs: ResultSet, column: String) = sqlArrayToList(rs.getArray(column))
  def getInt(rs: ResultSet, column: String) = rs.getInt(column)
  def getString(rs: ResultSet, column: String) = {
    val v = rs.getString(column)
    if (v == null) "" else v
  }

  def toDocument(rs: ResultSet)(implicit config: Config) = new Document(Map(
    (for(field <- config.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _) => IntField(getInt(rs, name))
       case StringFieldDef(name, _) => StringField(getString(rs, name))
       case ListFieldDef(name, _) => ListField(for(i <- getList(rs, name)) yield StringField(i))
     })
   ):_*
  ))
}
