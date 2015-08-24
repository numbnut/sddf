package de.unihamburg.vsis.sddf.indexing

import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable

/**
 * ATTENTION! This Blocker does NOT reduce the searchspace.
 * It just creates all possible tuple pairs.
 * Be aware of resulting quadratic complexety.
 */
class PipeIndexerDummy extends IndexingPipe {

  override val name = "DummyIndexer"
  
  def step(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): RDD[SymPair[Tuple]] = {
    val cartesian = input.cartesian(input).map(new SymPair(_))
    // filter identities like (a,a) and symmetric duplicates like (a,b) && (b,a)
    cartesian.filter(pair => pair._1 != pair._2).distinct()
  }
  
}

object PipeIndexerDummy {
  def apply() = {
    new PipeIndexerDummy()
  }
}
