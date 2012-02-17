package afm

import scala.collection.mutable._
import scala.math.min
import BitSetUtils._


trait Simhash {
  val bits = 32

  def features(str: String) = str.sliding(2).map(_.hashCode)

  def simhash(str: String): Int

  def rotatedSimhash(str: String, step: Int) = {
    val hash = simhash(str)
    for(i <- new Range(0, bits, step))
      yield Integer.toHexString(Simhash.rotated(hash, i)).reverse.padTo(bits/4, "0").reverse.mkString
  }

}

class AdditiveSimhash extends Simhash {
  def simhash(str: String) = features(str).reduceLeft(_ | _)
}

class MaxSimhash(val repeat: Int) extends Simhash {
  def simhash(str: String) = {
    val q = new PriorityQueue[Int]

    for(feature <- str.sliding(2).map(_.hashCode))
      q += feature

    var sim = 0
    if(q.length > 0)
      for(i <- 0 until min(repeat, q.length))
        sim ^= q.dequeue
    sim
  }
}

class BalancedSimhash extends Simhash {
  def simhash(str: String) = {
    val v = new Array[Int](bits)

    for(feature <- features(str)) {
      var n = feature
      for(b <- 0 until bits) {
        v(b) += (if((n & 1) == 1) 1 else -1)
        n = n >>> 1
      }
    }

    var sim = 0
    for(b <- v.map(_ > 0))
      sim = sim << 1 | (if(b) 1 else 0)
    sim
  }
}


object Simhash {
  val bits = 32

  def simhash = new AdditiveSimhash().simhash(_)

  def rotated(i: Int): Int = rotated(i, 1)
  def rotated(i: Int, bits: Int): Int = Integer.rotateLeft(i, bits)
}
