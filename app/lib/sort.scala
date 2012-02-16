package afm

import scala.sys.runtime
import scala.sys.process._
import scala.io._
import java.io._


class  Sorter(val inputFile: String, val outputFile: String) {

  lazy val lines = new BufferedSource(new FileInputStream(inputFile)).getLines.length

  def run {
    val cpus = runtime.availableProcessors

    val cmd = if (lines/cpus > 10000) "scripts/psort %s %s %s".format(inputFile, lines/cpus, cpus)
              else "sort %s".format(inputFile)
    
    println("sorting: %s".format(cmd))

    println("sorting")
    (cmd #> new java.io.File(outputFile) !!)
    println("sorting done")
  }
}
