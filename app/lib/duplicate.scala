package afm

import scala.collection.immutable.Queue


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

  def windowedDetect(docs: Seq[Document], windowSize: Int = Model.windowSize) = {
    var q = Queue[Document]()

    for(pivot <- docs)  {
      duplicatesInWindow(pivot, q)

      q = enqueue(q, pivot, windowSize)
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
