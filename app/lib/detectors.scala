package afm

import afm._
import afm.DbUtils._
import afm.MongoUtils

import com.mongodb.casbah.Imports._


trait Detector {
  val source = MongoConnection()("pace")("people")
  val collector = new MongoDBCollector("candidates")

  def run
}

class MongoStreamDetector(val key: String) extends Detector {
  def run {   
	  val rs = source.find().sort(Map(key -> 1)) map MongoUtils.toDocument
    Duplicates.windowedDetect(rs, collector, Model.windowSize)
  }
} 

class MongoSortedHashDetector(val hashes: Int) extends Detector {
  def run {
    val collector = new MongoDBCollector("candidates")
    
    for(i <- 0 to hashes)  {
      val key = "h%s".format(i)   

	    val rs = source.find().sort(Map(key -> 1)) map MongoUtils.toDocument
      Duplicates.windowedDetect(rs, collector, Model.windowSize)
    } 
  }
}

class MongoExternallySorted(val file: String) extends Detector {
  def run {

  }
}
