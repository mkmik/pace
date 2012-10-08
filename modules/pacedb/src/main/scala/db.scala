package afm

import com.twitter.querulous.evaluator.QueryEvaluator
import java.sql.ResultSet

import afm.model._
import afm.io._


object DbUtils {

  def sqlArrayToList(array: java.sql.Array) = 
    for(i <- (array.getArray.asInstanceOf[Array[String]]).toList)
      yield (if (i != null)  {
        val components = i.split("§§§")
        if(components.length > 1) {
          components(1)
        } else {
          i
        }
      } else { ""
            })

  def getList(rs: ResultSet, column: String) = sqlArrayToList(rs.getArray(column))
  def getInt(rs: ResultSet, column: String) = rs.getInt(column)
  def getString(rs: ResultSet, column: String) = {
    val v = rs.getString(column)
    if (v == null) "" else v
  }

  def toDocument(rs: ResultSet)(implicit config: Config) = new MapDocument(Map(
    (for(field <- config.fields)
     yield (field.name, field match {
       case IntFieldDef(name, _, _) => IntField(getInt(rs, name))
       case StringFieldDef(name, _, _) => StringField(getString(rs, name))
       case ListFieldDef(name, _, _) => ListField(for(i <- getList(rs, name)) yield StringField(i))
     })
   ):_*
  ))
}


class DBSource(implicit config: Config) extends Source {

  val queryEvaluator = QueryEvaluator("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/openaire", "dnet", "dnetPwd") 



  def documents(sortKey: String): Iterator[Document] =  {
    import DbUtils._

    queryEvaluator.select("select * from results_view_materialized order by dc_title") { row => toDocument(row) } iterator
  }

  def get[A](id: A): Option[Document] = throw new Exception("NIY")
  def get[A](ids: Seq[A]): Iterator[Document] = throw new Exception("NIY")

  def count: Long = queryEvaluator.select("select count(*) from results_view_materialized") { row => row.getLong(1) } head
  def count(query: Map[String, Any]): Long = queryEvaluator.select("select count(*) from results_view_materialized") { row => row.getLong(1) } head
}
