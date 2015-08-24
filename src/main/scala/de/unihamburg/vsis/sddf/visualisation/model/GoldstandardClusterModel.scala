package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class GoldstandardClusterModel extends BasicAnalysable {

  var _goldstandard: Option[RDD[Seq[Long]]] = None
  def goldstandard = _goldstandard
  def goldstandard_=(goldstandard: RDD[Seq[Long]]) = _goldstandard = Option(goldstandard)

  lazy val goldstandardClusterDistribution: Array[(Int, Int)] = {
    if (goldstandard.isDefined) {
      val gs: RDD[Seq[Long]] = goldstandard.get
      val unsertedClusterDistribution = gs.map(seq => (seq.size, 1)).reduceByKey(_ + _)
      unsertedClusterDistribution.collect.sortBy(_._1)
    } else {
      throw new Exception("No goldstandard present")
    }
  }

  lazy val duplicateCount: Int = {
    goldstandardClusterDistribution.map(p => (p._1 - 1) * p._2).reduce(_ + _)
  }

}
