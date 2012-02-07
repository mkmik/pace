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
}

trait Collector {
  def collect(dup: Duplicate)
}

class PrintingCollector extends Collector {
  def collect(dup: Duplicate) = println("DISTANCE %s".format(dup.d))
}

class MongoDBCollector(val collectionName: String) extends Collector {
  val coll = Model.mongoDb(collectionName)
  coll.drop()
  coll.ensureIndex(MongoDBObject("d" -> 1))

  def collect(dup: Duplicate) = coll += dup.toMongo
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
          //println("STOPPING ACTOR after adding %s dups".format(n))
          reply(n)
          exit('stop)
        }
      }
    }
  }

  def windowedDetect(docs: Iterable[Document], windowSize: Int = Model.windowSize) = {
    var n = 0

    var q = Queue[Document]()
    val executor = makeExecutor
    val pool = ExecutorScheduler(executor)
    //val collectorActor = makeCollectorActor(new PrintingCollector)
    val collector = new MongoDBCollector("candidates")
    val collectorActor = makeCollectorActor(collector)

    try {
      for(pivot <- docs)  {
        val records = q  // capture the reference to the current queue
        pool execute duplicatesInWindow(pivot, records, collectorActor)

        q = enqueue(q, pivot, windowSize)
        n += 1
        if (n % 100 == 0)
          println("---------------------------------------- %s".format(n))
      }

    } finally {
      //println("SHUTTING DOWN POOL")
      pool.shutdown()
      //println("WAITING FOR JOBS")
      executor.awaitTermination(10, TimeUnit.MINUTES)
      //println("SENDING STOP")
      val res = collectorActor !? Stop
      //println("GOT RES %s".format(res))
      //println("DONE")

      val coll = collector.coll
      println("DONE, CANDIDATES RETURNED BY COLLECTOR %s, IN DB %s".format(res, coll.count(MongoDBObject())))

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
