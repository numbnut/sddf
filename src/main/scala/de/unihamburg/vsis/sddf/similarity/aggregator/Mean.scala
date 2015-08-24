package de.unihamburg.vsis.sddf.similarity.aggregator

object Mean extends SimilarityAggregator {
  
  def agrSimilarity(vector: Array[Double]): Double = {
    vector.reduce(_ + _) / vector.size
  }
  
}
