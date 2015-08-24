package de.unihamburg.vsis.sddf.indexing.blocking

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilder
import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable

/**
 * Creates a SuffixArrayBlocker using the bkvBuilder function to create the
 * BlockingKeyValue (BKV) for each Tuple.
 */
class PipeBlockerSuffixArray(
  minimumSuffixLength: Int = 6,
  maximumBlockSize: Int = 12)(
    implicit bkvBuilder: BlockingKeyBuilder)
    extends BlockingPipe
    with Parameterized
    with Logging {

  @transient override val _analysable = new AlgoAnalysable
  _analysable.algo = this
  _analysable.name = this.name
  override val name = "SuffixArrayBlocker"
  override val paramMap = Map[String, Any]("minimumSuffixLength" -> minimumSuffixLength,
    "maximumBlockSize" -> maximumBlockSize, "BlockingKeyBuilder" -> bkvBuilder)

  /**
   * Calculate blocks of the given list of tuples.
   */
  def step(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): RDD[Seq[Tuple]] = {
    val bkvTuplePairs: RDD[(String, Tuple)] = input.map(t => (bkvBuilder.buildBlockingKey(t), t))
    val suffixTuplePairs: RDD[(String, Tuple)] = bkvTuplePairs.flatMap(pair => calcSuffixes(pair))
    val suffixBlocks: RDD[(String, Seq[Tuple])] = suffixTuplePairs.groupByKey().map(
      pair => (pair._1, pair._2.toSeq)
    )
    val filteredSuffixBlocks: RDD[(String, Seq[Tuple])] = suffixBlocks.filter(this.filterBlocks)

    log.debug("bkvTuplePairs count " + bkvTuplePairs.count)
    log.debug("SuffixTuplePairs count " + suffixTuplePairs.count)
    log.debug("SuffixBlocks count " + suffixBlocks.count)
    log.debug("filteredSuffixBlocks count " + filteredSuffixBlocks.count)

    filteredSuffixBlocks.map(_._2)
  }

  /**
   * Calculates the suffixes of a given BKV.
   * Longest suffix is the BKV it self.
   * Shortest suffix is the one with the size == minimumSuffixLength.
   */
  def calcSuffixes(bkvTuplePair: (String, Tuple)): Seq[(String, Tuple)] = {
    var result = scala.collection.mutable.MutableList[(String, Tuple)]()
    val bkv = bkvTuplePair._1
    val tuple = bkvTuplePair._2
    var i = 0
    var maxOffset = bkv.length() - minimumSuffixLength
    for (i <- 0 to maxOffset) {
      val suffix: String = bkv.substring(i)
      (suffix, tuple) +=: result
    }
    result
  }

  /**
   * Filter out all blocks with block size < 2 or block size > maximumBlockSize
   */
  def filterBlocks(suffixTuplePair: (String, Seq[Tuple])): Boolean = {
    val tupleCount = suffixTuplePair._2.length
    if (tupleCount > maximumBlockSize) {
      false
    } else if (tupleCount < 2) {
      false
    } else {
      true
    }
  }
}

object PipeBlockerSuffixArray {

  def apply(minimumSuffixLength: Int = 6, maximumBlockSize: Int = 12)(
    implicit bkvBuilder: BlockingKeyBuilder) = {
    new PipeBlockerSuffixArray(minimumSuffixLength, maximumBlockSize)
  }

}
