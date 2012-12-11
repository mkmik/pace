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

  val alpha = Set(('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9'): _*)
  val numbers = Set(('0' to '9'): _*)
  val aliases = Map(('₁' to '₉') zip ('1' to '9'): _*) ++ Map(('⁴' to '⁹') zip ('4' to '9'): _*) ++ Map('¹' -> '1', '²' -> '2', '³' -> '3')

  def cleanup(s: String) = removeSymbols(fixAliases(s).replaceAll("&ndash;", " ").replaceAll("&amp;", " ").replaceAll("&minus;", " ")).replaceAll("([0-9]+)", " $1 ").trim().replaceAll("""(?m)\s+""", " ").replaceAll("""\\n""", " ")

  def removeSymbols(s: String) = for (ch <- s) yield if (alpha.contains(ch)) ch else ' '
  def fixAliases(s: String) = for (ch <- s) yield if (aliases.contains(ch)) aliases(ch) else ch

  def getNumbers(s: String) = s.filter(ch => numbers.contains(ch))
  def isRoman(s: String) = s.replaceAll("""^M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$""", "qwertyuiop") == "qwertyuiop"
  def getRomans(s: String) = Set(s.split(" ").filter(isRoman): _*)

  def checkNumbers(a: String, b: String) = getNumbers(a) != getNumbers(b) || getRomans(a) != getRomans(b)

  def distance(a: String, b: String): Double = {
    val (ca, cb) = (cleanup(a), cleanup(b))
    if (checkNumbers(ca, cb)) 0.5 else normalize(ssalgo.score(ca, cb))
  }
  def distance(a: List[String], b: List[String]): Double = distance(concat(a), concat(b))

  def distance[A](a: Field[A], b: Field[A]): Double = (a, b) match {
    case (StringField(av), StringField(bv)) => distance(av, bv)
    case (ListField(av), ListField(bv)) => distance(av.asInstanceOf[List[StringField]].map(_.value), bv.asInstanceOf[List[StringField]].map(_.value))
    case _ => throw new Exception("invalid types")
  }

  def normalize(d: Double): Double = d
}

/*! The distance between two documents is given by the weighted mean of the field distances
 */
class DistanceScorer(val fields: List[FieldDef[_]]) {
  def distance(a: Document, b: Document) = {

    def fieldDistance(a: Document, b: Document, i: FieldDef[_]) = {
      if (i.algo.weight == 0) { // optimization for 0 weight
        0
      } else {
        // TODO: check for existence
        val va = a(i.name).getOrElse(EmptyField())
        val vb = b(i.name).getOrElse(EmptyField())
        if (va.isEmpty || vb.isEmpty) {
          if (i.ignoreMissing)
            0
          else
            1
        } else {
          i.algo.weight * i.algo.distance(va, vb)
        }
      }
    }

    val w = fields.map(_.algo.weight).sum
    (for (i <- fields)
      yield fieldDistance(a, b, i)).sum / w
  }
}

/*! Then we just have to define concrete instances based on second string
 */
case class JaroWinkler(w: Double) extends SecondStringDistanceAlgo(w, new com.wcohen.ss.JaroWinkler())

case class Level2JaroWinkler(w: Double) extends SecondStringDistanceAlgo(w, new com.wcohen.ss.Level2JaroWinkler())

/*! Some second string distance algorithms don't return the value in the correct range, so we need to normalize it.
 */
case class Levenstein(w: Double) extends SecondStringDistanceAlgo(w, new com.wcohen.ss.Levenstein()) {
  override def normalize(d: Double) = 1 / pow((abs(d) + 1), 0.1)
}

case class Level2Levenstein(w: Double) extends SecondStringDistanceAlgo(w, new com.wcohen.ss.Level2Levenstein()) {
  override def normalize(d: Double) = 1 / pow((abs(d) + 1), 0.1)
}

