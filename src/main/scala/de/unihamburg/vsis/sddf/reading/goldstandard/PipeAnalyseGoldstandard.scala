package de.unihamburg.vsis.sddf.reading.goldstandard

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.ResultContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardModel

class PipeAnalyseGoldstandard extends PipeElementPassthrough[RDD[SymPair[Tuple]]] {

  override val _analysable = new GoldstandardModel

  def substep(input: RDD[SymPair[Tuple]])(implicit pipeContext: AbstractPipeContext): Unit = {
    _analysable.goldstandard = input
    pipeContext match {
      case pc: ResultContext => {
        pc.goldstandardModel = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

object PipeAnalyseGoldstandard {

  def apply() = new PipeAnalyseGoldstandard()

}