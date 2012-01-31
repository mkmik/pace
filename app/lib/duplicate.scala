package afm

import scala.collection.immutable.Queue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Executors

import scala.actors.scheduler.ExecutorScheduler

object Duplicates {
  def detect(docs: Seq[Document]) = {
    var n = 0

    for(pivot <- docs)  {
      duplicatesInWindow(pivot, docs)

      if (n % 100 == 0)
        println("---------------------------------------- %s".format(n))
      n = n + 1
    }
  }

  val cpus = Runtime.getRuntime.availableProcessors

  def makePool = ExecutorScheduler(new ThreadPoolExecutor(cpus, cpus, 4, TimeUnit.SECONDS,
                                                        new LinkedBlockingQueue(20 + 2 * cpus),
                                                        Executors.defaultThreadFactory,
                                                        new ThreadPoolExecutor.CallerRunsPolicy()
                                                      ))

  def windowedDetect(docs: Seq[Document], windowSize: Int = Model.windowSize) = {
    var q = Queue[Document]()


    val pool = makePool

    try {
      for(pivot <- docs)  {
        pool execute duplicatesInWindow(pivot, q)

        q = enqueue(q, pivot, windowSize)
      }

    } finally {
      pool.shutdown()
      pool.join()
    }
  }

  def duplicatesInWindow(pivot: Document, records: Seq[Document]) = {
    for (r <- records) {
      if (pivot != r) {
        val d = DistanceAlgo.distance(pivot, r)
        if (d > Model.threshold)
          //println("DISTANCE %s for:\n\t'%s'\n\t'%s'\n".format(d, pivot, r))
          println("DISTANCE %s".format(d))
      }
    }
  }

  def enqueue[A] (q: Queue[A], v: A, windowSize: Int) = (if(q.length >= windowSize) q.tail else q).enqueue(v)
}
