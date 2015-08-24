package de.unihamburg.vsis.sddf.classification

import de.unihamburg.vsis.sddf.SddfContext.Duplicate
import de.unihamburg.vsis.sddf.SddfContext.FeatureId
import de.unihamburg.vsis.sddf.SddfContext.NoDuplicate
import de.unihamburg.vsis.sddf.SddfContext.Similarity
import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.SddfContext.Threshold
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext

class PipeClassificationThreshold(
    implicit thresholds: Array[(FeatureId, Threshold)])
  extends PipeElement[SymPairSim, SymPairSim] {

  def step(input: SymPairSim)(implicit pipeContext: AbstractPipeContext): SymPairSim = {
    pipeContext match {
      case pc: CorpusContext with GoldstandardContext => {
        val prediction = input.map(pair => (pair, decide(pair._2)))
        val filtered = prediction.filter(_._2 == Duplicate)
        val duplicatePairs = filtered.map(_._1)
        duplicatePairs
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

  private def decide(simVector: Array[Similarity]): Double = {
    val thresholdSimilarity: Seq[(Threshold, Similarity)] = thresholds.map(_._2).zip(simVector)
    val duplicateList = thresholdSimilarity.map(pair => if (pair._1 > pair._2) NoDuplicate else Duplicate)
    duplicateList.reduce(aggregateDuplicateValue)
  }

  private def aggregateDuplicateValue(val1: Double, val2: Double): Double = {
    if (val1 == NoDuplicate || val2 == NoDuplicate) {
      NoDuplicate
    } else {
      Duplicate
    }
  }

}

object PipeClassificationThreshold {
  def apply()(implicit thresholds: Array[(FeatureId, Threshold)]) = {
    new PipeClassificationThreshold()
  }
}