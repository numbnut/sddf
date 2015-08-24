package de.unihamburg.vsis.sddf.similarity.aggregator

/**
 * Selects the median. In case of a even dimensionality it takes the lower one.
 */
object Median extends SimilarityAggregator {
  
  def agrSimilarity(vector: Array[Double]): Double = {
    val medianIndex = vector.size / 2
    val sortedVector = vector.sortWith((a,b) => a > b)
    sortedVector(medianIndex)
  }
  
}
