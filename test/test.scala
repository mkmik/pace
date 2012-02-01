package test

import org.specs2.mutable._
import com.twitter.querulous.evaluator.QueryEvaluator

import afm._
import afm.DbUtils._


object DbSpec extends Specification {

  val queryEvaluator = QueryEvaluator("org.postgresql.Driver", "jdbc:postgresql://localhost:5433/openaire", "dnet", "dnetPwd")

  "the db" should {
    "handle arrays" in {
      val rs = queryEvaluator.select("select * from results_view order by dc_title") { row => toDocument(row) }

      //println(rs(0))
      //println(rs(1))
      //println(DistanceAlgo.distance(rs(0), rs(1)))
      for(i <- 0 to 0) {
        val subset = rs.take(3000)
        Duplicates.windowedDetect(subset, subset.length)
      }
      //Duplicates.detect(rs.take(1000))

      "test" must startWith("test")
    }
  }
}
