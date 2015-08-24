package de.unihamburg.vsis.sddf.similarity.measures

import com.rockymadden.stringmetric.StringMetric

final case class MeasureWrapperToLower(metric: StringMetric[Double]) extends StringMetric[Double] {
  
  /*
   * TODO this is very inefficient Array[Char] -> String
   */
  override def compare(a: Array[Char], b: Array[Char]): Option[Double] = {
    compare(a.toString(), b.toString())
  }

  override def compare(a: String, b: String): Option[Double] = metric.compare(a.toLowerCase(), b.toLowerCase())
}