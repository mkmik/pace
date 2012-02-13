package test

import org.specs2.mutable._
import com.twitter.querulous.evaluator.QueryEvaluator

import afm._
import afm.DbUtils._

import com.mongodb.casbah.Imports._


object DbSpec extends Specification {

  val queryEvaluator = QueryEvaluator("org.postgresql.Driver", "jdbc:postgresql://localhost:5433/openaire", "dnet", "dnetPwd")

  "the db" should {
    "handle arrays" in {

      //val detector = new MongoStreamDetector("n")
      val detector = new MongoExternallySorted("/tmp/hashes.sorted")
      detector.run

      "test" must startWith("test")
    }
  }
}
