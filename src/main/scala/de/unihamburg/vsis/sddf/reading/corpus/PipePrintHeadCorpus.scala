package de.unihamburg.vsis.sddf.reading.corpus

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.PipeSampler
import de.unihamburg.vsis.sddf.visualisation.Table
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

/**
 * Prints out the n first Tuples in the corpus.
 * This is much more efficient than sampling.
 */
class PipePrintHeadCorpus(count: Int = 10)(implicit fIdNameM: FeatureIdNameMapping)
  extends PipeElementPassthrough[RDD[Tuple]] with PipeSampler {

  def substep(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: CorpusContext => {
        val sample: Array[Tuple] = pc.corpus.take(count)
        val table: Seq[Seq[String]] = createTupleTable(sample)
        log.info("Corpus sample of " + sample.size + " tuples: ")
        Table.printTable(table)
      }
    }
  }

}

object PipePrintHeadCorpus {

  def apply(count: Int = 10)(implicit fIdNameM: FeatureIdNameMapping) = {
    new PipePrintHeadCorpus(count)
  }

}