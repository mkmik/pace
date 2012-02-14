package afm

import com.mongodb.casbah.Imports._


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


class MongoFeatureExtractor[A](val extractor: FeatureExtractor[A], val fileName: String) {
  val source = MongoConnection()("pace")("people")

  def run {
    run(Model.limit)
  }

  def run(limit: Option[Int]) {
    val sink = new java.io.PrintWriter(new java.io.File(fileName))

	  val rs = source.find() map MongoUtils.toDocument
    var n = 0

    def scan {
      for(doc <- rs) {
        if(limit match { case Some(x) => n > x; case None => false })
          return

        for(f <- extractor.extract(doc))
          sink.println("%s:%s".format(f.toString.trim, doc.fields("n").asInstanceOf[IntField].value))

        n += 1
        if (n % 1000 == 0)
          println("f--------------------------------------- %s".format(n))
      }
    }

    try {
      scan
    } finally {
      sink.close()
    }
  }
}
