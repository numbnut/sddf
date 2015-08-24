package de.unihamburg.vsis.sddf.similarity.measures

import com.rockymadden.stringmetric.StringMetric

case object MeasureEquality extends StringMetric[Double] {
  
  override def compare(a: Array[Char], b: Array[Char]): Option[Double] = {
    if (a.deep == b.deep) {
      Some(1.0)
    } else {
      Some(0.0)
    }
  }

  override def compare(a: String, b: String): Option[Double] = {
    if (a == b) {
      Some(1.0)
    } else {
      Some(0.0)
    }
  }
}