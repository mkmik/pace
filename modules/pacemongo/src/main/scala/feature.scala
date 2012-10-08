package afm.mongo.feature

import com.mongodb.casbah.Imports._
import java.io._
import resource._
import scala.math.round

import afm._
import afm.model._
import afm.io._
import afm.util._
import afm.duplicates._
import afm.distance._
import afm.feature._


class MongoFeatureExtractor[A](val extractor: FeatureExtractor[A], val fileName: String)(implicit val config: Config) extends ParallelCollector[Seq[String]] {
  def run {
    run(config.limit)
  }

  def run(limit: Option[Int]) {
    val totalRecords = config.source.count
    val allDocs = config.source.documents(config.identifierField)

    val limitedDocs = limit match {
      case Some(x) => allDocs.take(x)
      case None => allDocs
    }

    val docs = new ProgressReportingIterator(limitedDocs, "Features", Some(totalRecords))

    for(sink <- managed(new PrintWriter(new File(fileName)))) {
      object fileCollector extends GenericCollector[Seq[String]] {
        def collect(lines: Seq[String]) = for(line <- lines) sink.println(line)
      }

      runWithCollector(fileCollector) {
        (pool, collectorActor) =>
          for(page <- docs.grouped(60)) {
            pool execute {
              collectorActor ! (for(doc <- page;
                                    f <- extractor.extract(doc))
                                yield "%s:%s".format(f.toString.trim, doc.identifier))
            }
          }
      }
    }
  }
}
