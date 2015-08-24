package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

trait RecallAndPrecision {

  def recall(goldstandard: RDD[SymPair[Tuple]], result: RDD[SymPair[Tuple]]): Double = {
    recallNaive(goldstandard, result)
  }
  
  def precision(goldstandard: RDD[SymPair[Tuple]], result: RDD[SymPair[Tuple]]): Double = {
    precisionNaive(goldstandard, result)
  }
  
  /**
   * Compute the recall in distributed manner. For every partition a partial result is computed and
   * afterwards aggregated to the overall recall.
   * TODO the gold standard is collected and send to every node, which is not that efficient.
   * The function is limited to a gold standards which fits into the memory of every executor.
   */
  private def recallDistributed(goldstandard: RDD[SymPair[Tuple]], result: RDD[SymPair[Tuple]]): Double = {
    val gs = goldstandard.collect().toSet
    val partitionwiseIntersection = result.mapPartitions(iterator => {
      val intersectionAbsolute = iterator.count(pair => gs.contains(pair))
      Seq(intersectionAbsolute).iterator
    })
    val intersectionAbsolute = partitionwiseIntersection.reduce(_ + _)
    intersectionAbsolute.toDouble / goldstandard.count()
  }

  /**
   * This is a naive implementation of recall computation using the intersection operation of RDDs.
   * This is very inefficient compared to recallDistributed(...).
   */
  private def recallNaive(goldstandard: RDD[SymPair[Tuple]], result: RDD[SymPair[Tuple]]): Double = {
    val intersectionCount = goldstandard.intersection(result).count()
    intersectionCount.toDouble / goldstandard.count()
  }

  /**
   * Compute the precision in distributed manner. For every partition a partial result is computed
   * and afterwards aggregated to the overall precision.
   * TODO the gold standard is collected and send to every node, which is not that efficient.
   * The function is limited to a gold standards which fits into the memory of every executor.
   */
  private def precisionDistributed(
    goldstandard: RDD[SymPair[Tuple]],
    result: RDD[SymPair[Tuple]]): Double = {
    val gs = goldstandard.collect().toSet
    val partitionwiseIntersection = result.mapPartitions(iterator => {
      val intersectionAbsolute = iterator.count(pair => gs.contains(pair))
      Seq(intersectionAbsolute).iterator
    })
    val intersectionAbsolute = partitionwiseIntersection.reduce(_ + _)
    intersectionAbsolute.toDouble / result.count()
  }

  /**
   * This is a naive implementation of precision computation using the intersection operation of
   * RDDs. This is very inefficient compared to precisionDistributed(...).
   */
  private def precisionNaive(goldstandard: RDD[SymPair[Tuple]], result: RDD[SymPair[Tuple]]): Double = {
    val intersectionCount = result.intersection(goldstandard).count
    intersectionCount.toDouble / result.count()
  }
}