package afm

import com.mongodb.casbah.Imports._
import java.io._
import resource._
import scala.math.round


trait FeatureExtractor[A] {
  def extract(doc: Document): Seq[A]
}

trait ValueExtractor[A] {
  def extractValue(field: Field): Seq[A]
}

class FieldFeatureExtractor[A](val field: FieldDef[A]) extends FeatureExtractor[A] {
  self: ValueExtractor[A] =>

  def extract(doc: Document): Seq[A] = extractValue(doc.fields(field.name))
}

trait NGramValueExtractor extends ValueExtractor[String] {
  def extractValue(field: Field): Seq[String] = {
    field match {
      case StringField(value) => value.sliding(Model.ngramSize).take(Model.maxNgrams).toSeq
      case _ => throw new Exception("unsupported field type")
    }
  }
}


trait SimhashValueExtractor extends ValueExtractor[String] {
  def extractValue(field: Field): Seq[String] = {
    field match {
      case StringField(value) => {
        val hash = Simhash.simhash(value)
        for(i <- 0 until Simhash.bits)
          yield Integer.toHexString(Simhash.rotated(hash, i)).padTo(Simhash.bits/4, "0").mkString
      }
      case _ => throw new Exception("unsupported field type")
    }
  }
}


class MongoFeatureExtractor[A](val extractor: FeatureExtractor[A], val fileName: String) extends ParallelCollector[String]{
  val source = MongoConnection()("pace")("people")

  def run {
    run(Model.limit)
  }

  def run(limit: Option[Int]) {
    val totalRecords = source.count
    val allDocs = source.find().sort(Map("n" -> 1)) map MongoUtils.toDocument

    val limitedDocs = limit match {
      case Some(x) => allDocs.take(x)
      case None => allDocs
    }

    val docs = new ProgressReportingIterator(limitedDocs, "Features", Some(totalRecords))

    for(sink <- managed(new PrintWriter(new File(fileName)))) {
      object fileCollector extends GenericCollector[String] {
        def collect(line: String) = sink.println(line)
      }

      runWithCollector(fileCollector) {
        (pool, collectorActor) =>
          for(doc <- docs) {
            pool execute {
              for(f <- extractor.extract(doc))
                collectorActor ! "%s:%s".format(f.toString.trim, doc.fields("n").asInstanceOf[IntField].value)
            }
          }
      }
    }
  }
}
