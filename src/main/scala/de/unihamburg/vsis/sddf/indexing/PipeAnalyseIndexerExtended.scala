package de.unihamburg.vsis.sddf.indexing

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.pipe.context.ResultContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModelExtended

class PipeAnalyseIndexerExtended extends PipeElementPassthrough[RDD[SymPair[Tuple]]] {

  override val _analysable: IndexingModelExtended = new IndexingModelExtended

  def substep(input: RDD[SymPair[Tuple]])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: GoldstandardContext with CorpusContext with ResultContext => {
        _analysable.pairs = input
        _analysable.goldstandard = pc.goldstandard
        _analysable.corpus = pc.corpus
        pc.indexingModel = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

object PipeAnalyseIndexerExtended {
  
  def apply() = new PipeAnalyseIndexerExtended
  
}