package afm.io

import java.util.concurrent._
import scala.actors.scheduler.ExecutorScheduler
import scala.sys.runtime

import scala.actors.Actor
import scala.actors.Actor._

import afm._


trait GenericCollector[A] {
  def collect(dup: A)
}


trait CollectingActor[A] {
  case class Stop()

  def makeCollectorActor(collector: GenericCollector[A]): Actor = actor {
    var n = 0
    loop {
      react {
        case Stop => {
          reply(n)
          exit('stop)
        }
        case dup => {
          n += 1
          collector.collect(dup.asInstanceOf[A])
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
        case dup => {
          n += 1
          collector.collect(dup.asInstanceOf[A])
          reply(true)
        }
      }
    }
  }
}

trait ParallelCollector[A] extends CollectingActor[A] with ConfigProvider {
  implicit val ec = afm.io.ParallelCollector.ec

  val cpus = runtime.availableProcessors

  def threads = config.cores.getOrElse(cpus) * boost
  def boost = config.threadPoolBoost

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

object ParallelCollector {
  import akka.dispatch._
  import java.util.concurrent.Executors
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

}