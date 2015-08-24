package de.unihamburg.vsis.sddf.similarity

import com.rockymadden.stringmetric.StringMetric

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

object SimilarityCalculator extends Serializable {
  /**
   * TODO Handle missing features correctly
   * Return type should be Array[Option[Double]]
   */
  def similarity(
    a: Tuple,
    b: Tuple,
    featureMeasures: Array[(Int, StringMetric[Double])]
  ): Array[Double] = {
    featureMeasures.map(pair => {
      val featureId = pair._1
      val metric = pair._2
      val featureA = a.readFeature(featureId)
      val featureB = b.readFeature(featureId)
      if (featureA.isDefined && featureB.isDefined) {
        metric.compare(featureA.get, featureB.get).getOrElse(0D)
      } else {
        0D
      }
    })
  }

  def similarity(
    tuplePair: (Tuple, Tuple), 
    featuresMetrics: Array[(Int, StringMetric[Double])]
  ): Array[Double] = {
    similarity(tuplePair._1, tuplePair._2, featuresMetrics)
  }

  def similarity(
    tuplePair: SymPair[Tuple],
    featuresMetrics: Array[(Int, StringMetric[Double])]
  ): Array[Double] = {
    similarity(tuplePair._1, tuplePair._2, featuresMetrics)
  }

}
