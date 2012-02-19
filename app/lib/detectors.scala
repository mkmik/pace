package afm

import afm._
import afm.DbUtils._

import com.mongodb.casbah.Imports._
import java.util.concurrent.LinkedBlockingQueue

import java.io._
import scala.util.parsing.input.{StreamReader,Reader}
import scala.io._

import com.mongodb.Mongo

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import au.com.bytecode.opencsv.CSVReader


case class Metrics(val precision: Double, val recall: Double, val dups: Int)

trait Detector {
  val options = new MongoOptions()
  options.connectionsPerHost = 40
  val source = new MongoConnection(new Mongo("127.0.0.1", options))("pace")("people")

  def run: Metrics
}

class MongoStreamDetector(val key: String)(implicit collector: MongoDBCollector, implicit val config: Config) extends Detector {
  def run = {
    val rs = source.find().sort(Map(key -> 1)) map MongoUtils.toDocument

    val (docs, totalRecords) = config.limit match {
      case Some(x) => (rs.take(x), x)
      case None => (rs, source.count.toInt)
    }

    Duplicates.windowedDetect(docs, collector, config.windowSize, totalRecords=Some(totalRecords))
  }
}

class MongoSortedHashDetector(val hashes: Int, val totalRecords: Option[Long] = None)(implicit config: Config) extends Detector {
  def run = {
    val collector = new MongoDBCollector("candidates")

    for(i <- 0 to hashes)  {
      val key = "h%s".format(i)

      val rs = source.find().sort(Map(key -> 1)) map MongoUtils.toDocument
      Duplicates.windowedDetect(rs, collector, config.windowSize, totalRecords=totalRecords)
    }

    Metrics(0.0, 0.0, 0)
  }
}

class MongoExternallySorted(val file: String, val totalRecords: Option[Long] = None)(implicit collector: MongoDBCollector, implicit val config: Config) extends Detector {
  def run = {
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

    Duplicates.windowedDetect(new RandomAccessIterator(), collector, config.windowSize, totalRecords=totalRecords)
  }
}


class PrefetchingMongoExternallySorted(val file: String, val totalRecords: Option[Long] = None)(implicit collector: MongoDBCollector, implicit val config: Config) extends Detector {
  def run = {
    val sortedHashes = new BufferedSource(new FileInputStream(file))
    val lines = sortedHashes.getLines

    class PrefetchingRandomAccessIterator extends Iterator[Document] {
      var page: List[Document] = List()

      val pageSize = 60

      def hasNext = page.nonEmpty || lines.hasNext
      def next = {
        if(page.isEmpty)
          page = fetchPage

        val res = page.head
        page = page.tail
        res
      }

      def fetchPage = (source.find("n" $in fetchIds) map MongoUtils.toDocument).toList

      def fetchIds = (lines.take(pageSize) map getId).toSeq

      def getId(line: String) = Integer.parseInt(line.split(":")(1))
    }

    Duplicates.windowedDetect(new PrefetchingRandomAccessIterator(), collector, config.windowSize, totalRecords=totalRecords)
  }
}


class ParalellFetchMongoExternallySorted(val file: String, val totalRecords: Option[Long] = None)(implicit collector: MongoDBCollector) extends Detector with ParallelCollector[Document] {
  override def threads = 20

  def run = {
    val sortedHashes = new BufferedSource(new FileInputStream(file))
    val lines = sortedHashes.getLines

    object fifoCollector extends GenericCollector[Document] {
      val q = new FIFO[Document](new LinkedBlockingQueue(10000))

      def collect(doc: Document) = q.enqueue(doc)
    }

    import akka.dispatch.Future

    val fifoWorker = Future {
      runWithCollector(fifoCollector) {
        def getId(line: String) = Integer.parseInt(line.split(":")(1))

        (pool, collectorActor) =>
          for(page <- lines.grouped(6)) {
            val p = page
            pool.execute {
              for (doc <- (source.find("n" $in page.map(getId)) map MongoUtils.toDocument))
                fifoCollector.collect(doc)
            }
          }
      }

      println("Closing queue")
      fifoCollector.q.close
    }

    Duplicates.windowedDetect(fifoCollector.q.toIterator, collector, config.windowSize, totalRecords=totalRecords)
  }

}


class CmdlineMongoExternallySorted(val file: String, val totalRecords: Option[Long] = None)(implicit collector: MongoDBCollector) extends Detector with ParallelCollector[Document] {
  override def threads = 16

  def run = {
    val sortedHashes = new BufferedSource(new FileInputStream(file))
    val lines = sortedHashes.getLines

    object fifoCollector extends GenericCollector[Document] {
      val q = new FIFO[Document](new LinkedBlockingQueue(10000))

      def collect(doc: Document) = q.enqueue(doc)
    }

    import akka.dispatch.Future

    val fifoWorker = Future {
      runWithCollector(fifoCollector) {
        def getId(line: String) = Integer.parseInt(line.split(":")(1))

        (pool, collectorActor) =>
          for(page <- lines.grouped(2000)) {
            val p = page
            pool.execute {

              val q = "{n: {$in: [%s]}}".format((for(l <- page) yield getId(l).toString).reduceLeft(_ + "," + _))

              import scala.sys.process._

              val csv: String = (List("mongoexport", "-d", "pace", "-c", "people", "-f", "n,firstName,lastName,country,birthDate,kind,relatedTo", "--csv", "-q", q) !!)

                try {
                  val values:Seq[Array[String]] = new CSVReader(new java.io.StringReader(csv)).readAll.toSeq.drop(1)
                  val docs = values map CSVUtils.toDocument

                  for(doc <- docs)
                    fifoCollector.collect(doc)
                } catch {
                  case e => println("GOT exception %s: %s".format(e, csv))
                }
            }
          }
      }

      println("Closing queue")
      fifoCollector.q.close
    }

    Duplicates.windowedDetect(fifoCollector.q.toIterator, collector, config.windowSize, totalRecords=totalRecords)
  }

}


// List("mongoexport", "-d", "pace", "-c", "people", "--jsonArray", "-q", "{n: {$in: [10, 100, 1000]}}") !!
