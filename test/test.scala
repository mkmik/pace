package test

import org.specs2.mutable._

import afm._
import afm.Watch._
import resource._
import java.io._
import scala.sys.runtime


object DbSpec extends Specification {

  def reportToCsv(metrics: Metrics, time: Long) {
    val size = Model.limit match {
      case Some(x) => x
      case None => Model.mongoDb("people").count
    }
    val cores = Model.cores.getOrElse(runtime.availableProcessors)

    val reportFileName = Model.conf.getString("pace.reportFile").getOrElse("/tmp/pace.csv")
    val exists = new File(reportFileName).exists

    for(report <- managed(new PrintWriter(new FileWriter(reportFileName , true)))) {
      if(!exists)
        report.println("size,window, cores, threshold,time,precision,recall,candidates")

      val Metrics(precision, recall, candidates) = metrics
      report.println((size, Model.windowSize, cores, Model.threshold, time/1000, precision, recall, candidates).productIterator.map(_.toString).mkString(","))
    }
  }

  "pace" should {
    "rule" in {
      Model.fields must not be empty

      println("running")
      val (metrics, time) = timeTook { Model.scanner.run }
      println("done")

      reportToCsv(metrics, time)

      "test" must startWith("test")
    }
  }
}
