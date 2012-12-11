package afm.scanner

import afm._
import afm.feature._
import afm.model._
import afm.distance._
import afm.detectors._
import afm.duplicates._

trait Scanner extends ConfigProvider {
//  implicit val collector = config.collector
  def run: Metrics
}
