package test

import org.specs2.mutable._

import afm._

object DbSpec extends Specification {

  "pace" should {
    "rule" in {
      val featuresFile = "/tmp/ngrams.txt"
      val sortedFeaturesFile = "/tmp/ngrams.sorted"


      val features = new FieldFeatureExtractor(StringFieldDef("lastName", NullDistanceAlgo())) with NGramValueExtractor
      val feature = new MongoFeatureExtractor(features, featuresFile)
      feature.run

      val sorter = new Sorter(featuresFile, sortedFeaturesFile)
      sorter.run

      val lines = sorter.lines

      //val runner = new MongoStreamDetector("n", Some(lines))
      //val runner = new MongoExternallySorted(sortedFeaturesFile)
      val runner = new PrefetchingMongoExternallySorted(sortedFeaturesFile, Some(lines))
      //val runner = new ParalellFetchMongoExternallySorted(sortedFeaturesFile, Some(lines))
      //val runner = new CmdlineMongoExternallySorted(sortedFeaturesFile, Some(lines))
      runner.run

      "test" must startWith("test")
    }
  }
}
