package afm

import scala.sys.runtime
import scala.sys.process._
import scala.io._
import java.io._

class Sorter(val inputFile: String, val outputFile: String)(implicit config: Config) {

  lazy val lines = new BufferedSource(new FileInputStream(inputFile)).getLines.length

  def run {
    val cpus = runtime.availableProcessors
    val threads = config.cores.getOrElse(cpus)

    val cmd = if (lines / cpus > 10000) "scripts/psort %s %s %s".format(inputFile, lines / threads, threads)
    else "sort %s".format(inputFile)

    println("sorting: %s".format(cmd))

    println("sorting")
    (cmd #> new java.io.File(outputFile) !!)
    println("sorting done")
  }
}
