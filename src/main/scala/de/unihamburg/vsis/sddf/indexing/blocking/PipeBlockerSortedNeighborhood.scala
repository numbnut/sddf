package de.unihamburg.vsis.sddf.indexing.blocking

import org.apache.spark.mllib.rdd.RDDFunctions.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions

import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilder
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable

class PipeBlockerSortedNeighborhood(windowSize: Int = 10)(implicit bkvBuilder: BlockingKeyBuilder)
    extends BlockingPipe
    with Parameterized {

  def step(tuples: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): RDD[Seq[Tuple]] = {
    val bkvTuplePairs: RDD[(String, Tuple)] = tuples.map(t => (bkvBuilder.buildBlockingKey(t), t))
    val sortedPairs = bkvTuplePairs.sortByKey().map(_._2)
    sortedPairs.sliding(windowSize).map(_.toSeq)
  }

  @transient override val _analysable = new AlgoAnalysable
  _analysable.algo = this
  _analysable.name = this.name
  override val name = "SortedNeighborhoodBlocker"
  override val paramMap = Map("windowSize" -> windowSize,
    "BlockingKeyBuilder" -> bkvBuilder)

}

object PipeBlockerSortedNeighborhood {

  def apply(windowSize: Int = 10)(implicit bkvBuilder: BlockingKeyBuilder) = {
    new PipeBlockerSortedNeighborhood(windowSize)
  }

}
