package afm.distance

import scala.math._

import afm._
import afm.model._


/*! Each field is configured with a distance algo which knows how to compute
 the distance (0-1) between the fields of two objects. */
trait DistanceAlgo {
  val weight: Double

  def distance[A](a: Field[A], b: Field[A]): Double
}

/*! Not all fields of a document need to partecipate in the distance measure.
 We model those fields as having a NullDistanceAlgo. */
case class NullDistanceAlgo() extends DistanceAlgo {
  val weight = 0.0
  def distance[A](a: Field[A], b: Field[A]): Double = 0.0
}


/*! For the rest of the fields delegate the distance measure to the second string library
 */
abstract class SecondStringDistanceAlgo(val weight: Double, val ssalgo: com.wcohen.ss.AbstractStringDistance) extends DistanceAlgo {
  def concat(l: List[String]) = l.mkString(" ")

  val alpha = Set(('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9') :_*)
  val numbers = Set(('0' to '9') :_*)
  val aliases = Map(('₁' to '₉') zip ('1' to '9') :_*) ++ Map(('⁴' to '⁹') zip ('4' to '9') :_*) ++ Map('¹' -> '1', '²' -> '2', '³' -> '3' )


  def cleanup(s: String) = removeSymbols(fixAliases(s)).replaceAll("&ndash;", " ").replaceAll("&amp;", " ").replaceAll("&minus;", " ").trim().replaceAll("""(?m)\s+""", " ").replaceAll("""\\n""", " ")

  def removeSymbols(s: String) = for (ch <- s) yield if (alpha.contains(ch)) ch else ' '
  def fixAliases(s: String) = for (ch <- s) yield if (aliases.contains(ch)) aliases(ch) else ch

  def getNumbers(s: String) = s.filter(ch => numbers.contains(ch))
  def checkNumbers(a: String, b: String) = getNumbers(a) != getNumbers(b)

  def distance(a: String, b: String): Double = if(checkNumbers(a, b)) 0.5 else ssalgo.score(cleanup(a), cleanup(b))
  def distance(a: List[String], b: List[String]): Double = distance(concat(a), concat(b))

  def distance[A](a: Field[A], b: Field[A]): Double = (a, b) match {
    case (StringField(av), StringField(bv)) => distance(av, bv)
    case (ListField(av), ListField(bv)) => distance(av.asInstanceOf[List[StringField]].map(_.value), bv.asInstanceOf[List[StringField]].map(_.value))
    case _ => throw new Exception("invalid types")
  }
}

/*! The distance between two documents is given by the weighted mean of the field distances
 */
object DistanceAlgo {
  def distance(a: Document, b: Document)(implicit config: Config) = {

    def fieldDistance(a: Document, b: Document, i: FieldDef[_]) = {
      if (i.algo.weight == 0) { // optimization for 0 weight
        0
      } else {
        val va = a(i.name).get
        val vb = b(i.name).get
        if(i.ignoreMissing && (va.isEmpty || vb.isEmpty))
          1
        else
          i.algo.weight * i.algo.distance(va, vb)
      }
    }

    val w = config.fields.map(_.algo.weight).sum
    (for(i <- config.fields)
     yield fieldDistance(a, b, i)).sum / w
  }
}

/*! Then we just have to define concrete instances based on second string
 */
case class JaroWinkler(w: Double) extends SecondStringDistanceAlgo(w, new com.wcohen.ss.JaroWinkler())

/*! Some second string distance algorithms don't return the value in the correct range, so we need to normalize it.
 */
case class Levenstein(w: Double) extends SecondStringDistanceAlgo(w, new com.wcohen.ss.Levenstein()) {
  override def distance(a: String, b: String) = 1/pow((abs(super.distance(a, b)) + 1), 0.1)
}

