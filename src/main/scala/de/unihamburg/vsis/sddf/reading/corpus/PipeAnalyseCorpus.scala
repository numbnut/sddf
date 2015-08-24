package de.unihamburg.vsis.sddf.reading.corpus

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.IdConverter
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.TupleArray
import de.unihamburg.vsis.sddf.visualisation.model.ReadingModel
import de.unihamburg.vsis.sddf.pipe.context.ResultContext

class PipeAnalyseCorpus
  extends PipeElementPassthrough[RDD[Tuple]]
  with Serializable {

  override val _analysable = new ReadingModel

  def substep(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): Unit = {
    _analysable.tuples_=(input)
    pipeContext match {
      case pc: ResultContext => {
        pc.readingModel = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

object PipeAnalyseCorpus {
  def apply() = {
    new PipeAnalyseCorpus()
  }
}