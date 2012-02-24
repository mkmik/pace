package afm.scanner

import afm._
import afm.feature._
import afm.model._
import afm.distance._
import afm.detectors._
import afm.duplicates._


trait Scanner extends ConfigProvider {
  implicit val collector = config.collector
  def run: Metrics
}


class SingleFieldScanner(implicit val config: Config) extends Scanner {
  def run = new MongoStreamDetector(config.sortOn).run
}


trait FeaturedScanner {
  self: Scanner =>

  def multiPass[A](featuresFile: String, sortedFeaturesFile: String, extractor: FeatureExtractor[A]) = {
    new MongoFeatureExtractor(extractor, featuresFile).run

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

class NgramScanner(implicit val config: Config) extends Scanner with FeaturedScanner {
  def run = {
    val features = new FieldFeatureExtractor(StringFieldDef(config.compareOn, NullDistanceAlgo())) with NGramValueExtractor
    multiPass("/tmp/ngrams.txt", "/tmp/ngrams.sorted", features)
  }
}

class MergedSimhashScanner(implicit val config: Config) extends Scanner with FeaturedScanner {
  def run = {
    val features = new FieldFeatureExtractor(StringFieldDef(config.compareOn, NullDistanceAlgo())) with RotatedSimhashValueExtractor
    multiPass("/tmp/simhash.txt", "/tmp/simhash.sorted", features)
  }
}

trait MultiPassScanner[A] extends Scanner {
  def extractorForIteration(i: Int): FeatureExtractor[A]

  def simhash(featuresFile: String, sortedFeaturesFile: String) = {
    var last = Metrics(0.0, 0.0, 0)

    for(i <- 0 until 8)  {
      val extractor = extractorForIteration(i)

      val feature = new MongoFeatureExtractor(extractor, featuresFile.format(i))
      feature.run
      
      val sorter = new Sorter(featuresFile.format(i), sortedFeaturesFile.format(i))
      sorter.run
      
      val lines = sorter.lines
      
      val runner = new PrefetchingMongoExternallySorted(sortedFeaturesFile.format(i), Some(lines))
      val metrics = runner.run
      println("Increase in: Precision: %s, Recall: %s, Dups %s".format(metrics.precision - last.precision, metrics.recall - last.recall, metrics.dups - last.dups))
      last = metrics
    }

    last 
  }

}

class MultiPassSimhashScanner(implicit val config: Config) extends Scanner with MultiPassScanner[String] {
  def run = simhash("/tmp/simhash-%s.txt", "/tmp/simhash-%s.sorted")

  def extractorForIteration(i: Int) = new FieldFeatureExtractor(StringFieldDef(config.compareOn, NullDistanceAlgo())) with SimhashValueExtractor {
    def step = i
  }

}
