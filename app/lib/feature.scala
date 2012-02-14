package afm

import com.mongodb.casbah.Imports._

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


class MongoFeatureExtractor[A](val extractor: FeatureExtractor[A], val fileName: String) extends ParallelCollector[String]{
  val source = MongoConnection()("pace")("people")

  def run {
    run(Model.limit)
  }

  def run(limit: Option[Int]) {
    val sink = new java.io.PrintWriter(new java.io.File(fileName))

    val totalRecords = source.count
    val allDocs = source.find() map MongoUtils.toDocument
    var n = 0

    val docs = limit match {
      case Some(x) => allDocs.take(x)
      case None => allDocs
    }

    object FileCollector extends GenericCollector[String] {
      def collect(line: String) = sink.println(line)
    }

    try {
      runWithCollector(FileCollector) {
        (pool, collectorActor) =>
          for(doc <- docs) {
            pool execute {
              for(f <- extractor.extract(doc))
                collectorActor ! "%s:%s".format(f.toString.trim, doc.fields("n").asInstanceOf[IntField].value)
            }

            n += 1
            if (n % 1000 == 0) {
              val percent = "(%s%%)".format(round(100.0 * n / totalRecords))
              println("F--------------------------------------- %s %s".format(n, percent))
            }
          }
      }
    } finally {
      sink.close()
    }
  }
}
