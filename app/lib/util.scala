package afm

import scala.math.round


class ProgressReportingIterator[A](val iterator: Iterator[A], val label: String = "", val totalRecords: Option[Long] = None) extends Iterator[A] {
  var n = 0 
  def hasNext = iterator.hasNext
  def next = {
    n += 1
    if (n % Model.progressStep == 0) {
      val percent = totalRecords match {
        case Some(t) => "(%s%%)".format(round(100.0 * n / t))
        case None => ""
      }
      println("%s %s %s".format(label.padTo(40, "-").mkString, n, percent))
    }

    iterator.next
  }
}
