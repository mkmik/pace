package afm

import scala.math.round
import org.scala_tools.time.Imports._
import org.joda.time.format.PeriodFormatterBuilder


class ProgressReportingIterator[A](val iterator: Iterator[A], val label: String = "", val totalRecords: Option[Long] = None)(implicit config: Config) extends Iterator[A] {
  var n = 0
  var lastTime = System.currentTimeMillis
  val mav = new MovingAverage[Double](50)

  def hasNext = iterator.hasNext
  def next = {
    n += 1
    if (n % config.progressStep == 0) {
      val took = System.currentTimeMillis - lastTime
      lastTime = System.currentTimeMillis
      val rps = round(config.progressStep.toDouble / took * 1000)
      val arps = mav(rps)

      val fb = new PeriodFormatterBuilder().appendDays().appendSuffix("d ").appendHours().appendSuffix("h ").appendMinutes().appendSuffix("m ").appendSeconds().appendSuffix("s").printZeroNever().toFormatter()

      val percent = totalRecords match {
        case Some(t) => "(%s%%, ETA: %s)".format(round(100.0 * n / t), fb.print(new Period(round(1000.0 * (t-n)/arps))))
        case None => ""
      }
      println("%s %s (RPS: %s, ARPS: %s) %s".format(label.padTo(40, "-").mkString, n, rps.toString.padTo(8, " ").mkString, round(arps).toString.padTo(8, " ").mkString, percent))
    }

    iterator.next
  }
}

class MovingAverage[A: Fractional](period: Int) {
  private val queue = new scala.collection.mutable.Queue[A]()
  def apply(n: A)(implicit num: Fractional[A]) = {
    queue.enqueue(n)
    if (queue.size > period)
      queue.dequeue
    num.div(queue.sum, num.fromInt(queue.size))
  }
  def clear = queue.clear
}
