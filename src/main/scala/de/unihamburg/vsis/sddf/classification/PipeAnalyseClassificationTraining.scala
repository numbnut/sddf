package de.unihamburg.vsis.sddf.classification

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.ResultContext
import de.unihamburg.vsis.sddf.visualisation.model.TrainingSetModel

class PipeAnalyseClassificationTraining
  extends PipeElementPassthrough[(SymPairSim, RDD[LabeledPoint])] {

  override val _analysable: TrainingSetModel = new TrainingSetModel

  def substep(
      input: (SymPairSim, RDD[LabeledPoint]))(
      implicit pipeContext: AbstractPipeContext): Unit = {
    _analysable.trainingsSetLabeled = input._2
    pipeContext match {
      case pc: ResultContext => {
        pc.trainingSetModel = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

/**
 * companion object
 */
object PipeAnalyseClassificationTraining {

  def apply() = new PipeAnalyseClassificationTraining

}
