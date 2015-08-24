package de.unihamburg.vsis.sddf.similarity.measures

import com.rockymadden.stringmetric.StringMetric

final case class MeasureNumeric(maxValueForNormalisation: Int, minValueForNormalisation: Int = 0) extends StringMetric[Double] {
  
  val normalise = maxValueForNormalisation - minValueForNormalisation
  
  override def compare(a: Array[Char], b: Array[Char]): Option[Double] = {
    compare(a.toString(), b.toString())
  }

  override def compare(a: String, b: String): Option[Double] = {
    // make robust against empty strings
    val aVal = if(a.length > 0) a.toInt else 0
    val bVal = if(b.length > 0) b.toInt else 0
    val diff = math.abs(aVal - bVal)
    Some(1 - (diff.toDouble / normalise))
  }
}