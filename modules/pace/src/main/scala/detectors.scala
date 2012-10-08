package afm.detectors

import afm._
import afm.duplicates._

trait Detector {
  def run: Metrics
}
