package afm

import afm._
import afm.DbUtils._

import com.mongodb.casbah.Imports._

import java.io._
import scala.util.parsing.input.{StreamReader,Reader}
import scala.io._


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
    val sortedHashes = new BufferedSource(new FileInputStream(file))
    val lines = sortedHashes.getLines

    class RandomAccessIterator extends Iterator[Document] {
      def hasNext = lines.hasNext
      def next = {
        val line = lines.next
        val hash_id = line.split(":")
        val id = Integer.parseInt(hash_id(1))

        source.findOne(Map("n" -> id)) match {
          case Some(a) => MongoUtils.toDocument(a)
          case None => throw new Exception("cannot find id %s".format(id))
        }
      }
    }

    Duplicates.windowedDetect(new RandomAccessIterator(), collector, Model.windowSize)
  }
}
