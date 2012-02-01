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
  val coll = MongoDBCollector.mongoConn("afm")(collectionName)
  coll.drop()

  def collect(dup: Duplicate) = coll += dup.toMongo
}

object MongoDBCollector {
  val mongoConn = MongoConnection()
}


object Duplicates {
  val cpus = Runtime.getRuntime.availableProcessors

  def makePool = ExecutorScheduler(new ThreadPoolExecutor(cpus, cpus, 4, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue(20 + 2 * cpus),
                                                        Executors.defaultThreadFactory,
                                                        new ThreadPoolExecutor.CallerRunsPolicy()
                                                      ))

  case class Stop

  def makeCollectorActor(collector: Collector): Actor = actor {
    loop {
      react {
        case dup: Duplicate => collector.collect(dup)
        case Stop => {
          println("STOPPING ACTOR")
          reply(true)
          exit('stop)
        }
      }
    }
  }

  def windowedDetect(docs: Seq[Document], windowSize: Int = Model.windowSize) = {
    var n = 0

    var q = Queue[Document]()
    val pool = makePool
    //val collectorActor = makeCollectorActor(new PrintingCollector)
    val collectorActor = makeCollectorActor(new MongoDBCollector("candidates"))

    try {
      for(pivot <- docs)  {
        pool execute duplicatesInWindow(pivot, q, collectorActor)

        q = enqueue(q, pivot, windowSize)
        n += 1
        if (n % 100 == 0)
          println("---------------------------------------- %s".format(n))
      }

    } finally {
      println("SENDING STOP")
      println("SHUTTING DOWN POOL")
      pool.shutdown()
      println("WAITING FOR JOBS")
      pool.join()
      val res = collectorActor !? Stop
      println("GOT RES %s".format(res))
      println("DONE")
    }
  }

  def duplicatesInWindow(pivot: Document, records: Seq[Document], collectorActor: Actor) = {
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
