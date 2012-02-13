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

      val features = new FieldFeatureExtractor(StringFieldDef("lastName", NullDistanceAlgo())) with NGramValueExtractor
      val feature = new MongoFeatureExtractor(features, "/tmp/ngrams.txt")
      feature.run

      //val runner = new MongoStreamDetector("n")
      //val runner = new MongoExternallySorted("/tmp/hashes.sorted")
      val runner = new MongoExternallySorted("/tmp/ngrams.sorted")
      runner.run

      "test" must startWith("test")
    }
  }
}
