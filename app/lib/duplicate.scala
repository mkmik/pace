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

trait ParallelCollector[A] extends CollectingActor[A] {
  val cpus = Runtime.getRuntime.availableProcessors * 4

  def makeExecutor = new ThreadPoolExecutor(cpus, cpus, 4, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue(0 + 2 * cpus),
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

  def windowedDetect(docs: Iterator[Document], collector: MongoDBCollector, windowSize: Int = Model.windowSize, limit: Option[Int] = Model.limit) = {
    var n = 0

    var q = Queue[Document]()

    def scan(pool: ExecutorScheduler, collectorActor: Actor) {
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
      val res = runWithCollector(collector)(scan)
    } finally {
      println("DONE, CANDIDATES RETURNED BY COLLECTOR %s, IN DB %s".format(collector.dups, collector.coll.count(MongoDBObject())))
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
