package de.unihamburg.vsis.sddf.indexing.blocking

import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModel
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.ResultContext
import de.unihamburg.vsis.sddf.visualisation.model.BlockingModel

class PipeAnalyseBlocker extends PipeElementPassthrough[RDD[Seq[Tuple]]] {

  override val _analysable: BlockingModel = new BlockingModel

  def substep(input: RDD[Seq[Tuple]])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: GoldstandardContext with CorpusContext with ResultContext => {
        _analysable.blocks = input
        pc.blockingModel = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

object PipeAnalyseBlocker {
  
  def apply() = new PipeAnalyseBlocker
  
}