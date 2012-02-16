package test

import org.specs2.mutable._

import afm._
import afm.Watch._

object DbSpec extends Specification {

  "pace" should {
    "rule" in {
      val (res, time) = timeTook {
        println("running")
        Model.algo match {
          case "singleField" => {
            val runner = new MongoStreamDetector(Model.sortOn)
            runner.run
          }
          case "ngram" => {

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
          }
        }
      }

      println("res %s, time %s".format(res, time))

      "test" must startWith("test")
    }
  }
}
