package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import de.unihamburg.vsis.sddf.SddfContext.rddToRdd
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

/**
 * TODO need caching for relevant RDDs
 */
class ClusterModel extends BasicAnalysable with RecallAndPrecision {

  var _clusters: Option[RDD[Set[Tuple]]] = None
  def clusters = {
    if (_clusters.isDefined) {
      _clusters.get
    } else {
      throw new Exception("Pairs not defined")
    }
  }
  def clusters_=(clusters: RDD[Set[Tuple]]) = _clusters = Option(clusters)

  var _goldstandard: Option[RDD[SymPair[Tuple]]] = None
  def goldstandard = {
    if (_goldstandard.isDefined) {
      _goldstandard.get
    } else {
      throw new Exception("Gold standard not defined")
    }
  }
  def goldstandard_=(goldstandard: RDD[SymPair[Tuple]]) = _goldstandard = Option(goldstandard)

  /**
   * Returns the average cluster size
   */
  lazy val averageClusterSize: Double = {
    clusters.map(_.size).reduce(_ + _) / clusters.count().toDouble
  }

  /**
   * Returns an array of (size, count) pairs
   */
  lazy val clusterSizeDistribution: Array[(Int, Int)] = {
    val distr = clusters.map(set => (set.size, 1))
    val distr2 = distr.reduceByKey(_ + _)
    // distr should be small so using collect is fine right here
    distr2.sortByKey().collect()
  }
  
  lazy val duplicateCount: Int = {
    clusterSizeDistribution.map(p => {
      val clusterSize = p._1
      val clusterCount = p._2
      (clusterSize - 1) * clusterCount 
    }).sum
  }
  
  lazy val duplicatePairCount: Int = {
    clusterSizeDistribution.map(p => {
      val clusterSize = p._1
      val clusterCount = p._2
      numberOfPossiblePairs(clusterSize) * clusterCount
    }).sum
  }

  lazy val clusterCount = {
    clusters.count
  }

  lazy val recall: Double = {
    recall(goldstandard, getDuplicatePairs)
  }

  lazy val precision: Double = {
    precision(goldstandard, getDuplicatePairs)
  }

  private lazy val getDuplicatePairs: RDD[SymPair[Tuple]] = {
    clusters.map(_.toSeq).cartesianBlocksWithoutIdentity()
  }
  
  private def numberOfPossiblePairs(elementCount: Int): Int = {
    (elementCount * (elementCount - 1)) / 2
  }

}
