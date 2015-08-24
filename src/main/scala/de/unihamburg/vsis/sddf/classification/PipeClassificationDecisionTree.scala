package de.unihamburg.vsis.sddf.classification

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.SddfContext.Duplicate
import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable
import de.unihamburg.vsis.sddf.Parameterized
import org.apache.spark.mllib.classification.ClassificationModel

class PipeClassificationDecisionTree(
    impurity: String = "gini",
    maxDepth: Int = 5,
    maxBins: Int = 32)
  extends AbstractPipeClassification {

  val paramMap: Map[String, Any] = Map(("impurity", impurity), ("maxDepth", maxDepth), ("maxBins", maxBins))

  def trainModelAndClassify(
    trainingData: RDD[LabeledPoint],
    symPairSim: SymPairSim): RDD[(SymPair[Tuple], Array[Double], Double)] = {
    val model = DecisionTree.trainClassifier(trainingData, numClasses = 2,
      categoricalFeaturesInfo = Map[Int, Int](), impurity, maxDepth, maxBins)

    log.debug("Decision Tree Model:" + model)
    log.debug("Decision Tree:" + model.toDebugString)

    // Marking Missing Values as Not Equal (0)
    symPairSim.map(pair => (pair._1, pair._2, model.predict(Vectors.dense(pair._2))))
  }

}

object PipeClassificationDecisionTree {
  def apply(
    impurity: String = "gini",
    maxDepth: Int = 5,
    maxBins: Int = 32) = {
    new PipeClassificationDecisionTree(impurity, maxDepth, maxBins)
  }
}