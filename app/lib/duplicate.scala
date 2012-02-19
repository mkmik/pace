package afm

import scala.collection.immutable.Map
import scala.collection.immutable.Queue
import java.util.concurrent._
import scala.actors.scheduler.ExecutorScheduler

import scala.actors.Actor
import scala.actors.Actor._

import scala.math.round
import scala.sys.runtime

import com.mongodb.casbah.Imports._


case class Duplicate(val d: Double, val a: Document, val b: Document) {
  def toMongo = MongoDBObject("d" -> d,
                              "left" -> a.toMongo,
                              "right" -> b.toMongo)

  def check = a.realIdentifier == b.realIdentifier
}

trait GenericCollector[A] {
  def collect(dup: A)
}

trait Collector extends GenericCollector[Duplicate] {
  var truePositives = 0
  var dups = 0

  def precision = truePositives.asInstanceOf[Double] / dups
  def recall = truePositives.asInstanceOf[Double] / realDups

  def realDups: Long
}

class PrintingCollector extends Collector {
  def collect(dup: Duplicate) = println("DISTANCE %s".format(dup.d))
  def realDups = 0
}

class MongoDBCollector(val collectionName: String)(implicit config: Config) extends Collector {
  val coll = config.mongoDb(collectionName)
  coll.drop()
  coll.ensureIndex(MongoDBObject("d" -> 1))

  var seen: Set[String] = Set()

  def collect(dup: Duplicate) {
    val max = List(dup.a.identifier.toString, dup.b.identifier.toString).max
    val min = List(dup.a.identifier.toString, dup.b.identifier.toString).min
    val seenKey = "%s%s".format(max, min)

    if (!(seen contains seenKey)) {
      coll += dup.toMongo
      dups += 1
      if(dup.check)
        truePositives += 1
      seen = seen + seenKey
    }
  }

  def shrinkingFactor: Double = config.limit match {
    case Some(l) => l.toDouble / config.mongoDb("people").count.toDouble
    case None => 1.0
  }

  def realDups = (config.mongoDb("people").count(MongoDBObject("kind" -> "duplicate")).toDouble * shrinkingFactor).toLong
}

trait CollectingActor[A] {
  case class Stop

  def makeCollectorActor(collector: GenericCollector[A]): Actor = actor {
    var n = 0
    loop {
      react {
        case Stop => {
          reply(n)
          exit('stop)
        }
        case dup: A => {
          n += 1
          collector.collect(dup)
        }
      }
    }
  }
}

trait BlockingCollectingActor[A] extends CollectingActor[A] {
  override def makeCollectorActor(collector: GenericCollector[A]): Actor = actor {
    var n = 0
    loop {
      react {
        case Stop => {
          reply(n)
          exit('stop)
        }
        case dup: A => {
          n += 1
          collector.collect(dup)
          reply(true)
        }
      }
    }
  }
}

trait ParallelCollector[A] extends CollectingActor[A] {
  val cpus = runtime.availableProcessors

  val config = implicitly[OverrideConfig]

  def threads = config.cores.getOrElse(cpus) * boost
  def boost = config.conf.getInt("pace.threadPoolBoost").getOrElse(4)

  def makeExecutor = new ThreadPoolExecutor(threads, threads, 4, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue(0 + 8 * threads),
                                                        Executors.defaultThreadFactory,
                                                        new ThreadPoolExecutor.CallerRunsPolicy()
                                                      )

  def runWithCollector[B](collector: GenericCollector[A])(body: (ExecutorScheduler, Actor) => B): B = {
    val executor = makeExecutor
    val pool = ExecutorScheduler(executor)
    val collectorActor = makeCollectorActor(collector)

    try {
      body(pool, collectorActor)
    } finally {
      println("Waiting for executor")
      pool.shutdown()
      executor.awaitTermination(10, TimeUnit.MINUTES)
      val res = collectorActor !? Stop
      println("Executor finished")
      res
    }
  }
}

object Duplicates extends ParallelCollector[Duplicate] {

  def windowedDetect(allDocs: Iterator[Document], collector: MongoDBCollector,
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

  def report(collector: MongoDBCollector) = {
    val precision = collector.precision
    val recall = collector.recall
    val dups = collector.dups

    println("DONE, CANDIDATES RETURNED BY COLLECTOR %s, IN DB %s".format(collector.dups, collector.coll.count(MongoDBObject())))
    println("WINDOW SIZE %s, INPUT LIMIT %s".format(config.windowSize, config.limit))
    println("THREADS %s".format(threads))
    println("FOUND DUPS %s".format(dups))
    println("REAL  DUPS %s (shrinking factor %s)".format(collector.realDups, collector.shrinkingFactor))
    println("TRUE POSITIVES %s".format(collector.truePositives))
    println("PRECISION %s".format(precision))
    println("RECALL %s".format(recall))

    Metrics(precision, recall, dups)
  }

  def duplicatesInWindow(pivot: Document, window: Iterable[Document], collectorActor: Actor) = {
    for (r <- window) {
      if (pivot.identifier != r.identifier) {
        val d = DistanceAlgo.distance(pivot, r)
        if (d > config.threshold)
          collectorActor ! Duplicate(d, pivot, r)
      }
    }
  }

  def enqueue[A] (q: Queue[A], v: A, windowSize: Int) = (if(q.length >= windowSize) q.tail else q).enqueue(v)
}
