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
 * Creates a StandardBlocker using the bkvBuilder function to create the
 * BlockingKeyValue (BKV) for each Tuple.
 */
class PipeBlockerStandard(implicit bkvBuilder: BlockingKeyBuilder)
    extends BlockingPipe
    with Parameterized
    with Logging {

  /**
   * Calculate blocks of the given list of tuples.
   */
  def step(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): RDD[Seq[Tuple]] = {
    val bkvTuplePairs: RDD[(String, Tuple)] = input.map(t => (bkvBuilder.buildBlockingKey(t), t))
    val keyBlocks: RDD[(String, Iterable[Tuple])] = bkvTuplePairs.groupByKey
    keyBlocks.map(_._2.toSeq).filter(_.size > 1)
  }

  @transient override val _analysable = new AlgoAnalysable
  _analysable.algo = this
  _analysable.name = this.name
  override val name = "StandardBlocker"
  override val paramMap = Map("BlockingKeyBuilder" -> bkvBuilder)

}

object PipeBlockerStandard {

  def apply(implicit bkvBuilder: BlockingKeyBuilder) = {
    new PipeBlockerStandard()
  }

}
