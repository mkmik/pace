package afm

import scala.collection.mutable.BitSet
import BitSetUtils._


object Simhash {
  val bits = 32

  def slowSimhash(str: String) = {
    val v = new Array[Int](bits)

    for(feature <- str.sliding(2).map(_.hashCode).map(_.toBitSet)) {
      for(b <- feature)
        v(b) += (if(feature(b)) 1 else -1)
    }

    var sim = 0
    for(b <- v.map(_ > 0))
      sim = sim << 1 | (if(b) 1 else 0)
    sim
  }

  def simhash(str: String) = {
    val v = new Array[Int](bits)

    for(feature <- str.sliding(2).map(_.hashCode)) {
      var n = feature
      for(b <- 0 until bits) {
        v(b) += (if((n & 1) == 1) 1 else 0)
        n = n >>> 1
      }
    }

    var sim = 0
    for(b <- v.map(_ > 0))
      sim = sim << 1 | (if(b) 1 else 0)
    sim
  }


  def rotated(i: Int): Int = rotated(i, 1)
  def rotated(i: Int, bits: Int): Int = Integer.rotateLeft(i, bits)
}
