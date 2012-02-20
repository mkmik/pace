package afm.distance

import scala.math._

import afm._
import afm.model._


abstract class DistanceAlgo(val weight: Double, val ssalgo: com.wcohen.ss.AbstractStringDistance) {
  def concat(l: List[String]) = l.reduceLeft(_ + " " + _)

  def distance(a: String, b: String): Double = ssalgo.score(a, b)
  def distance(a: List[String], b: List[String]): Double = distance(concat(a), concat(b))

  def distance[A](a: Field[A], b: Field[A]): Double = (a, b) match {
    case (StringField(av), StringField(bv)) => distance(av, bv)
    case (ListField(av:List[StringField]), ListField(bv:List[StringField])) => distance(av.map(_.value), bv.map(_.value))
    case _ => throw new Exception("invalid types")
  }
}

object DistanceAlgo {
  def distance(a: Document, b: Document)(implicit config: Config) = {
    val w = config.fields.map(_.algo.weight).reduceLeft(_ + _)
    (for(i <- config.fields)
     yield (if (i.algo.weight == 0) 0
            else
              i.algo.weight * i.algo.distance(a.fields(i.name), b.fields(i.name)))).reduceLeft(_ + _) / w
  }
}


case class Levenstein(w: Double) extends DistanceAlgo(w, new com.wcohen.ss.Levenstein()) {
  override def distance(a: String, b: String) = 1/pow((abs(super.distance(a, b)) + 1), 0.1)
}

case class JaroWinkler(w: Double) extends DistanceAlgo(w, new com.wcohen.ss.JaroWinkler())

case class NullDistanceAlgo() extends DistanceAlgo(0, null) {
  override def distance(a: String, b: String): Double = 0.0
}
