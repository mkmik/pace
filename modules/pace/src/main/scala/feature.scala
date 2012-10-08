package afm.feature

//import com.mongodb.casbah.Imports._
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

  def extract(doc: Document): Seq[A] = extractValue(doc(field.name) match {
    case Some(f) => f
    case None => throw new Exception("Cannot get field %s".format(field.name))
  })
}

trait NGramValueExtractor extends ValueExtractor[String] {
  def extractValue(field: Field[String])(implicit config: Config): Seq[String] = {
    field match {
      case StringField(value) => value.sliding(config.ngramSize).take(config.maxNgrams).map(_.replace(":","_")).toSeq
      case _ => throw new Exception("unsupported field type")
    }
  }
}

trait TokenizedNGramValueExtractor extends ValueExtractor[String] {
  def extractValue(field: Field[String])(implicit config: Config): Seq[String] = {
    def tokenize(v: String) = v.split(" ")
    field match {
      case StringField(value) => (for(token: String <- tokenize(value).toSet;
				     ngram <- token.sliding(config.ngramSize).take(config.maxNgrams))
	yield ngram.replace(":","_")).toSeq
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
