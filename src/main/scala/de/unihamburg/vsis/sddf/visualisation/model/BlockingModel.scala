package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class BlockingModel extends BasicAnalysable with RecallAndPrecision {

  var _blocks: Option[RDD[Seq[Tuple]]] = None
  def blocks = {
    if (_blocks.isDefined) {
      _blocks.get
    } else {
      throw new Exception("Blocks not defined")
    }
  }
  def blocks_=(blocks: RDD[Seq[Tuple]]) = _blocks = Option(blocks)

  lazy val blockCount = blocks.count

  // (size, count)
  lazy val blockDistribution: Seq[(Int, Int)] = {
    blocks.map(block => (block.size, 1)).reduceByKey(_ + _).sortByKey().collect().toSeq
  }

  lazy val averageBlockSize = {
    blockDistribution.map(p => p._1 * p._2).sum / blockCount.toDouble
  }

}
