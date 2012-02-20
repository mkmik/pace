package afm.util

object Watch {
  def timed[A](caption: String)(body: => A) = {
    println("executing %s".format(caption))
    val (res, time) = timeTook(body)
    println("executed %s in %sms".format(caption, time))
    res
  }

  def timeTook[A](body: => A) = {
    val start = System.currentTimeMillis
    val res = body
    (res, System.currentTimeMillis - start)
  }
}
