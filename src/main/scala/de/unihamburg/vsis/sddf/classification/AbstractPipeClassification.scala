package de.unihamburg.vsis.sddf.classification

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.SddfContext.Duplicate
import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable

abstract class AbstractPipeClassification()
  extends PipeElement[(SymPairSim, RDD[LabeledPoint]), SymPairSim]
  with Parameterized {

  override val _analysable = new AlgoAnalysable
  _analysable.algo = this

  /**
   * Initially this was only the method to train the model but there is no common supertype
   * of all classifiers so we need to do the classification
   */
  def trainModelAndClassify(
    trainingData: RDD[LabeledPoint],
    symPairSim: SymPairSim): RDD[(SymPair[Tuple], Array[Double], Double)]

  def step(input: (SymPairSim, RDD[LabeledPoint]))(implicit pipeContext: AbstractPipeContext): SymPairSim = {
    pipeContext match {
      case pc: CorpusContext with GoldstandardContext => {

        val symPairSim = input._1
        val trainingsSet = input._2

        val prediction = trainModelAndClassify(trainingsSet, symPairSim)

        val duplicatePairs = prediction.filter(_._3 == Duplicate).map(tri => (tri._1, tri._2))

        duplicatePairs
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}