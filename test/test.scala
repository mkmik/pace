package test

import org.specs2.mutable._

import resource._
import java.io._
import scala.sys.runtime

import afm._
import afm.util.Watch._
import afm.model._
import afm.mongo._
import afm.feature._
import afm.duplicates._


object DbSpec extends Specification {

  def reportToCsv(metrics: Metrics, time: Long)(implicit config: OverrideConfig) {
    val size = config.limit match {
      case Some(x) => x
      case None => config.source.count
    }
    val cores = config.cores.getOrElse(runtime.availableProcessors)

    val reportFileName = config.conf.getString("pace.reportFile").getOrElse("/tmp/pace.csv")
    val exists = new File(reportFileName).exists

    for(report <- managed(new PrintWriter(new FileWriter(reportFileName , true)))) {
      if(!exists)
        report.println("size,window, cores, threshold,time,precision,recall,candidates")

      val Metrics(precision, recall, candidates) = metrics
      report.println((size, config.windowSize, cores, config.threshold, time/1000, precision, recall, candidates).productIterator.map(_.toString).mkString(","))
    }
  }

  "pace" should {
    "rule" in {
      implicit val config: OverrideConfig = new Object with ConfigurableModel

      config.fields must not be empty

      println("running")
      val (metrics, time) = timeTook { config.scanner.run }
      println("done")

      reportToCsv(metrics, time)

      "test" must startWith("test")
    }
  }
}

object DistanceSpec extends Specification {
  import afm.distance._

  val l = Levenstein(1)

  "Distance" should {
    "handle roman numerals" in {
      val a = "Hello I,IV"
      val b = "Hello II,IV"

      l.getRomans(l.cleanup(a)) must be equalTo(Set("I", "IV"))

      l.checkNumbers(l.cleanup(a), l.cleanup(b)) must be equalTo(true)

      l.distance(a, b) must be equalTo(0.5)
    }

    "handle special html entities" in {
      val a = "Investigating changes in basal conditions of Variegated Glacier prior to and during its 1982-1983 surge"
      val b = "Investigating changes in basal conditions of Variegated Glacier prior to and during its 1982&ndash;1983 surge"

      l.cleanup(a) must be equalTo(l.cleanup(b))
    }

    "cleanup symbols" in {
      val a = "Energy transfer mechanism and Auger effect in Er3+ coupled silicon nanoparticle samples"
      val b = "Energy transfer mechanism and Auger effect in Er(3+) coupled silicon nanoparticle samples"

      l.cleanup(a) must be equalTo(l.cleanup(b))
    }
  }
}
