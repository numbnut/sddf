package de.unihamburg.vsis.sddf.indexing

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.pipe.context.ResultContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModel

class PipeAnalyseIndexer extends PipeElementPassthrough[RDD[SymPair[Tuple]]] {

  override val _analysable: IndexingModel = new IndexingModel

  def substep(input: RDD[SymPair[Tuple]])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: CorpusContext with ResultContext => {
        _analysable.pairs = input
        _analysable.corpus = pc.corpus
        pc.indexingModel = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

object PipeAnalyseIndexer {
  
  def apply() = new PipeAnalyseIndexer
  
}