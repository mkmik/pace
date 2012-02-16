package afm

import scala.collection.mutable.BitSet


object BitSetUtils {
  def bitsetFromInt(i: Int) = {
    val bits = new BitSet

    var value = i
    var index = 0

    while (value != 0) {
      if ((value & 1) != 0)
        bits += index;

      index += 1;
      value = value >>> 1;
    }

    bits
  }

  def bitsetFromLong(i: Long) = {
    val bits = new BitSet

    var value = i
    var index = 0

    while (value != 0) {
      if ((value & 1) != 0)
        bits += index;

      index += 1;
      value = value >>> 1;
    }

    bits
  }


  def bitsetToInt(bits: BitSet) = {
    var value = 0
    for(b <- bits)
      value = value | 1 << b
    value
  }

  def bitsetToLong(bits: BitSet) = {
    var value = 0L
    for(b <- bits)
      value = value | 1 << b
    value
  }


  implicit def intToBitSetter(i: Int): IntBitSetter = new IntBitSetter(i)

  class IntBitSetter(val i: Int) {
    def toBitSet: BitSet = bitsetFromInt(i)
  }

  implicit def longToBitSetter(i: Long): LongBitSetter = new LongBitSetter(i)

  class LongBitSetter(val i: Long) {
    def toBitSet: BitSet = bitsetFromLong(i)
  }


  implicit def bitsetToBitGetter(bits: BitSet) = new BitGetter(bits)

  class BitGetter(bits: BitSet) {
    def toInt = bitsetToInt(bits)
    def toLong = bitsetToLong(bits)
  }
}
