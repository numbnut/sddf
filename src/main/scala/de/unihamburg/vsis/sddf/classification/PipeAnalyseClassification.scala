package de.unihamburg.vsis.sddf.classification

import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.ResultContext
import de.unihamburg.vsis.sddf.visualisation.model.ClassificationModel

class PipeAnalyseClassification extends PipeElementPassthrough[SymPairSim] {

  override val _analysable: ClassificationModel = new ClassificationModel

  def substep(input: SymPairSim)(implicit pipeContext: AbstractPipeContext): Unit = {
    _analysable.duplicatePairs = input
    pipeContext match {
      case pc: ResultContext => {
        pc.classificationModel = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

object PipeAnalyseClassification {
  def apply() = {
    new PipeAnalyseClassification()
  }
}