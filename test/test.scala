package test

import org.specs2.mutable._
import com.twitter.querulous.evaluator.QueryEvaluator

import afm._
import afm.DbUtils._

import com.mongodb.casbah.Imports._

import scala.sys.process._
import scala.io._
import java.io._

object DbSpec extends Specification {

  val queryEvaluator = QueryEvaluator("org.postgresql.Driver", "jdbc:postgresql://localhost:5433/openaire", "dnet", "dnetPwd")

  "the db" should {
    "handle arrays" in {

      val features = new FieldFeatureExtractor(StringFieldDef("lastName", NullDistanceAlgo())) with NGramValueExtractor
      val feature = new MongoFeatureExtractor(features, "/tmp/ngrams.txt")
      //feature.run

      println("sorting")

      val lines = new BufferedSource(new FileInputStream("/tmp/ngrams.txt")).getLines.length
      val cpus = Runtime.getRuntime.availableProcessors

      val cmd = if (lines/cpus > 10000) "scripts/psort /tmp/ngrams.txt %s %s".format(lines/cpus, cpus)
                else "sort /tmp/ngrams.txt"

      println("sorting: %s".format(cmd))
      cmd #> new java.io.File("/tmp/ngrams.sorted") !!

      println("sorting done")

      //val runner = new MongoStreamDetector("n", Some(lines))
      //val runner = new MongoExternallySorted("/tmp/hashes.sorted")
      //val runner = new PrefetchingMongoExternallySorted("/tmp/ngrams.sorted", Some(lines))
      val runner = new ParalellFetchMongoExternallySorted("/tmp/ngrams.sorted", Some(lines))
      runner.run

      "test" must startWith("test")
    }
  }
}
