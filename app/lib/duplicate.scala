package afm.duplicates

import scala.collection.immutable.Map
import scala.collection.immutable.Queue
import java.util.concurrent._
import scala.actors.scheduler.ExecutorScheduler

import scala.actors.Actor
import scala.actors.Actor._

import scala.math.round
import scala.sys.runtime

import com.mongodb.casbah.Imports._

import afm._
import afm.model._
import afm.io._
import afm.util._
import afm.feature._
import afm.distance._


case class Metrics(val precision: Double, val recall: Double, val dups: Int)

case class Duplicate(val d: Double, val a: Document, val b: Document) {

  def id: String = (a.identifier, b.identifier) match {
    case (xa: Int, xb: Int) => {
      val ids = List(xa, xb)
      "%s-%s".format(ids.max, ids.min)
    }
    case (xa: String, xb: String) => {
      val ids = List(xa, xb)
      "%s-%s".format(ids.max, ids.min)
    }
  }

  def check = a.realIdentifier == b.realIdentifier
}

trait Collector extends GenericCollector[Duplicate] with ConfigProvider {
  var truePositives = 0
  var dups = 0
  var seen: Set[String] = Set()

  def precision = truePositives.asInstanceOf[Double] / dups
  def recall = truePositives.asInstanceOf[Double] / realDups

  def realDups: Long

  def shrinkingFactor: Double = config.limit match {
    case Some(l) => l.toDouble / config.source.count.toDouble
    case None => 1.0
  }

  def append(dup: Duplicate)

  def collect(dup: Duplicate) {
    val dupId = dup.id
    if (!(seen contains dupId)) {
      append(dup)
      dups += 1
      if(dup.check)
        truePositives += 1
      seen = seen + dupId
    }
  }

}

class PrintingCollector(implicit val config: Config) extends Collector {
  def append(dup: Duplicate) = println("DISTANCE %s".format(dup.d))
  def realDups = 0
}

trait Duplicates extends ParallelCollector[Duplicate] {

  def windowedDetect(allDocs: Iterator[Document], collector: Collector,
                     windowSize: Int = config.windowSize, totalRecords: Option[Long] = None): Metrics

  def duplicatesInWindow(pivot: Document, window: Iterable[Document], collectorActor: Actor) = {
    for (r <- window) {
      if (pivot.identifier != r.identifier) {
        val d = DistanceAlgo.distance(pivot, r)
        if (d > config.threshold)
          collectorActor ! Duplicate(d, pivot, r)
      }
    }
  }

  def report(collector: Collector) = {
    val precision = collector.precision
    val recall = collector.recall
    val dups = collector.dups

    println("DONE, CANDIDATES RETURNED BY COLLECTOR %s".format(collector.dups))
    println("WINDOW SIZE %s, INPUT LIMIT %s".format(config.windowSize, config.limit))
    println("THREADS %s".format(threads))
    println("FOUND DUPS %s".format(dups))
    println("REAL  DUPS %s (shrinking factor %s)".format(collector.realDups, collector.shrinkingFactor))
    println("TRUE POSITIVES %s".format(collector.truePositives))
    println("PRECISION %s".format(precision))
    println("RECALL %s".format(recall))

    Metrics(precision, recall, dups)
  }
}

class SortedNeighborhood(implicit val config: Config) extends Duplicates {
  def windowedDetect(allDocs: Iterator[Document], collector: Collector,
                     windowSize: Int = config.windowSize, totalRecords: Option[Long] = None) = {

    val docs = new ProgressReportingIterator(allDocs, "Dups", totalRecords)

    var window = Queue[Document]()

    val res = runWithCollector(collector) {
      (pool, collectorActor) =>
        for(pivot <- docs)  {
          val w = window  // capture the reference to the current queue
          pool execute duplicatesInWindow(pivot, w, collectorActor)

          window = enqueue(window, pivot, windowSize)
        }
    }

    report(collector)
  }

  def enqueue(q: Queue[Document], v: Document, windowSize: Int) = (if(q.length >= windowSize) q.tail else q).enqueue(v)
}


class Blocking(implicit config: Config) extends SortedNeighborhood {
  override def enqueue(q: Queue[Document], v: Document, windowSize: Int) = {

    val blocking = new FieldFeatureExtractor(StringFieldDef(config.sortOn, NullDistanceAlgo())) with ValueExtractor[String] {
      def extractValue(field: Field[String])(implicit config: Config): Seq[String] = {
        field match {
          case StringField(value) => List(value.take(config.blockingPrefix))
          case _ => throw new Exception("unsupported field type")
        }
      }
    }

    val matchesPrev = q.isEmpty || blocking.extract(q.head).head == blocking.extract(v).head

    if(matchesPrev)
      q.enqueue(v)
    else
      Queue()
  }
}
