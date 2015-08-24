package de.unihamburg.vsis.sddf.similarity.aggregator

/**
 * Aggregates the similarity vector to a single value
 */
trait SimilarityAggregator {
  
  def agrSimilarity(vector: Array[Double]): Double
  
}
