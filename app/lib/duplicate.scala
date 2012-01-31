package afm


object Duplicates {
  def detect(docs: Iterable[Document]) = {
    var n = 0

    for(x <- docs;
        y <- docs)  {
      if (x != y) {
        val d = DistanceAlgo.distance(x, y)
        if (d > 0.9)
          println("DISTANCE %s for:\n\t'%s'\n\t'%s'\n".format(d, x, y))
      }
      
      if (n % 10000 == 0)
        println("---------------------------------------- %s".format(n))
      n = n + 1
    }
  }
}
