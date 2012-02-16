package test

import org.specs2.mutable._

import afm._

object DbSpec extends Specification {

  "the db" should {
    "handle arrays" in {

      val features = new FieldFeatureExtractor(StringFieldDef("lastName", NullDistanceAlgo())) with NGramValueExtractor
      val feature = new MongoFeatureExtractor(features, "/tmp/ngrams.txt")
      feature.run

      val sorter = new Sorter("/tmp/ngrams.txt", "/tmp/ngrams.sorted")
      sorter.run

      val lines = sorter.lines

      //val runner = new MongoStreamDetector("n", Some(lines))
      //val runner = new MongoExternallySorted("/tmp/hashes.sorted")
      val runner = new PrefetchingMongoExternallySorted("/tmp/ngrams.sorted", Some(lines))
      //val runner = new ParalellFetchMongoExternallySorted("/tmp/ngrams.sorted", Some(lines))
      //val runner = new CmdlineMongoExternallySorted("/tmp/ngrams.sorted", Some(lines))
      runner.run

      "test" must startWith("test")
    }
  }
}
