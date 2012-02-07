package test

import org.specs2.mutable._
import com.twitter.querulous.evaluator.QueryEvaluator

import afm._
import afm.DbUtils._
import afm.MongoUtils

import com.mongodb.casbah.Imports._


object DbSpec extends Specification {

  val queryEvaluator = QueryEvaluator("org.postgresql.Driver", "jdbc:postgresql://localhost:5433/openaire", "dnet", "dnetPwd")

  val source = MongoConnection()("pace")("people")

  "the db" should {
    "handle arrays" in {
      //val rs = queryEvaluator.select("select * from results_view order by dc_title") { row => toDocument(row) }
      val rs = source map MongoUtils.toDocument

      for(i <- 0 to 0) {
        val subset = rs.take(3000)
        Duplicates.windowedDetect(subset, subset.toSeq.length)
      }

      "test" must startWith("test")
    }
  }
}
