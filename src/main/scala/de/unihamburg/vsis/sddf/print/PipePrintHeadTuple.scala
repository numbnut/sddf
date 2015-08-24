package de.unihamburg.vsis.sddf.print

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.PipeSampler
import de.unihamburg.vsis.sddf.visualisation.Table

/**
 * Prints out the n first Tuples in the corpus.
 * This is much more efficient than sampling.
 */
class PipePrintHeadTuple(count: Int = 10)(implicit fIdNameM: FeatureIdNameMapping)
  extends PipeElementPassthrough[RDD[Tuple]] with PipeSampler {

  def substep(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): Unit = {
    val sample: Array[Tuple] = input.take(count)
    val table: Seq[Seq[String]] = createTupleTable(sample)
    log.info("Sample of " + sample.size + " tuples: ")
    Table.printTable(table)
  }

}

object PipePrintHeadTuple {

  def apply(count: Int = 10)(implicit fIdNameM: FeatureIdNameMapping) = {
    new PipePrintHeadTuple(count)
  }

}