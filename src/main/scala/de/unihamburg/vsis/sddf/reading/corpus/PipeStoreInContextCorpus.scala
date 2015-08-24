package de.unihamburg.vsis.sddf.reading.corpus

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable
import de.unihamburg.vsis.sddf.reading.Tuple

class PipeStoreInContextCorpus extends PipeElementPassthrough[RDD[Tuple]] {

  def substep(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: CorpusContext => pc.corpus = input
    }
  }
}

object PipeStoreInContextCorpus {

  def apply() = new PipeStoreInContextCorpus()

}