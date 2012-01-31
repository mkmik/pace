package afm

import com.twitter.querulous.evaluator.QueryEvaluator

import java.sql.ResultSet

//val rs = queryEvaluator.select("select * from results_view") { row => row.getArray("dc_creator").getArray.asInstanceOf[Array[String]](0)}.first.split("§§§")(1)

object DbUtils {

  def sqlArrayToList(array: java.sql.Array) = 
    for(i <- (array.getArray.asInstanceOf[Array[String]]).toList)
      yield (if (i != null)  {
        val components = i.split("§§§")
        if (components.length > 1)
          components(1)
        else
          ""
      } else { null
            })

  def getList(rs: ResultSet, column: String) = sqlArrayToList(rs.getArray(column))
  def getString(rs: ResultSet, column: String) = rs.getString(column)

  def toDocument(rs: ResultSet) = new Document(Map(
    (for(field <- Model.fields)
     yield (field.name, field match {
       case StringFieldDef(name, _) => StringField(rs.getString(name))
       case ListFieldDef(name, _) => ListField(for(i <- getList(rs, name)) yield StringField(i))
     })
   ):_*
  ))
}
