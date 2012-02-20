package afm.feature

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


trait FeatureExtractor[A] extends ConfigProvider {
  def extract(doc: Document): Seq[A]
}

trait ValueExtractor[A] {
  def extractValue(field: Field[A])(implicit config: Config): Seq[A]
}

class FieldFeatureExtractor[A](val field: FieldDef[A])(implicit val config: Config) extends FeatureExtractor[A] {
  self: ValueExtractor[A] =>

  def extract(doc: Document): Seq[A] = extractValue(doc(field.name).get)
}

trait NGramValueExtractor extends ValueExtractor[String] {
  def extractValue(field: Field[String])(implicit config: Config): Seq[String] = {
    field match {
      case StringField(value) => value.sliding(config.ngramSize).take(config.maxNgrams).toSeq
      case _ => throw new Exception("unsupported field type")
    }
  }
}


trait RotatedSimhashValueExtractor extends ValueExtractor[String] {
  def extractValue(field: Field[String])(implicit config: Config): Seq[String] = {
    field match {
      case StringField(value) => config.simhashAlgo.rotatedSimhash(value.toLowerCase, config.simhashRotationStep)
      case _ => throw new Exception("unsupported field type")
    }
  }
}

trait SimhashValueExtractor extends ValueExtractor[String] {
  def step: Int

  def extractValue(field: Field[String])(implicit config: Config): Seq[String] = {
    field match {
      case StringField(value) => {
        val hash = Integer.rotateLeft(config.simhashAlgo.simhash(value.toLowerCase), step * config.simhashRotationStep)
        List(Integer.toHexString(hash).reverse.padTo(Simhash.bits/4, "0").reverse.mkString)
      }
      case _ => throw new Exception("unsupported field type")
    }
  }
}


class MongoFeatureExtractor[A](val extractor: FeatureExtractor[A], val fileName: String)(implicit val config: Config) extends ParallelCollector[Seq[String]] {
  def run {
    run(config.limit)
  }

  def run(limit: Option[Int]) {
    val totalRecords = config.source.count
    val allDocs = config.source.documents("n")

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
                                yield "%s:%s".format(f.toString.trim, doc[Int]("n").get.value))
            }
          }
      }
    }
  }
}
