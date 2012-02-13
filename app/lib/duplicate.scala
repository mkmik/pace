package afm

import scala.collection.immutable.Map
import scala.collection.immutable.Queue
import java.util.concurrent._
import scala.actors.scheduler.ExecutorScheduler

import scala.actors.Actor
import scala.actors.Actor._

import com.mongodb.casbah.Imports._


case class Duplicate(val d: Double, val a: Document, val b: Document) {
  def toMongo = MongoDBObject("d" -> d,
                              "left" -> a.toMongo,
                              "right" -> b.toMongo)

  def check = a.realIdentifier == b.realIdentifier
}

trait Collector {
  def collect(dup: Duplicate)

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

class MongoDBCollector(val collectionName: String) extends Collector {
  val coll = Model.mongoDb(collectionName)
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

  def realDups = Model.mongoDb("people").count(MongoDBObject("kind" -> "duplicate"))
}


object Duplicates {
  val cpus = Runtime.getRuntime.availableProcessors

  def makeExecutor = new ThreadPoolExecutor(cpus, cpus, 4, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue(0 + 2 * cpus),
                                                        Executors.defaultThreadFactory,
                                                        new ThreadPoolExecutor.CallerRunsPolicy()
                                                      )

  case class Stop

  def makeCollectorActor(collector: Collector): Actor = actor {
    var n = 0
    loop {
      react {
        case dup: Duplicate => {
          n += 1
          collector.collect(dup)
        }
        case Stop => {
          reply(n)
          exit('stop)
        }
      }
    }
  }

  def windowedDetect(docs: Iterator[Document], collector: MongoDBCollector, windowSize: Int = Model.windowSize, limit: Option[Int] = Model.limit) = {
    var n = 0

    var q = Queue[Document]()
    val executor = makeExecutor
    val pool = ExecutorScheduler(executor)
    val collectorActor = makeCollectorActor(collector)

    def scan {
        for(pivot <- docs)  {
          if(limit match { case Some(x) => n > x; case None => false })
            return

          val records = q  // capture the reference to the current queue
          pool execute duplicatesInWindow(pivot, records, collectorActor)

          q = enqueue(q, pivot, windowSize)
          n += 1
          if (n % 1000 == 0)
            println("---------------------------------------- %s".format(n))
        }
    }

    try {
      scan
    } finally {
      pool.shutdown()
      executor.awaitTermination(10, TimeUnit.MINUTES)
      val res = collectorActor !? Stop

      val coll = collector.coll
      println("DONE, CANDIDATES RETURNED BY COLLECTOR %s, IN DB %s".format(res, coll.count(MongoDBObject())))
      println("FALSE POSITIVES %s".format(collector.dups))
      println("PRECISION %s".format(collector.precision))
      println("RECALL %s".format(collector.recall))
    }
  }

  def duplicatesInWindow(pivot: Document, records: Iterable[Document], collectorActor: Actor) = {
    for (r <- records) {
      if (pivot != r) {
        val d = DistanceAlgo.distance(pivot, r)
        if (d > Model.threshold)
          collectorActor ! Duplicate(d, pivot, r)
      }
    }
  }

  def enqueue[A] (q: Queue[A], v: A, windowSize: Int) = (if(q.length >= windowSize) q.tail else q).enqueue(v)
}
