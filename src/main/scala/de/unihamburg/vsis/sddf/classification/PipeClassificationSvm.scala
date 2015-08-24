package de.unihamburg.vsis.sddf.classification

import scala.beans.BeanInfo
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import org.apache.spark.mllib.classification.SVMWithSGD

class PipeClassificationSvm(numIterations: Int = 100) extends AbstractPipeClassification {

  val paramMap: Map[String, Any] = Map(("numIterations", numIterations))

    def trainModelAndClassify(
    trainingData: RDD[LabeledPoint],
    symPairSim: SymPairSim): RDD[(SymPair[Tuple], Array[Double], Double)] = {
    
    val model = SVMWithSGD.train(trainingData, numIterations)

    log.debug("Classification Model:" + model)

    // Marking Missing Values as Not Equal (0)
    symPairSim.map(pair => (pair._1, pair._2, model.predict(Vectors.dense(pair._2))))
  }

}

object PipeClassificationSvm {
  def apply(numIterations: Int = 100) = {
    new PipeClassificationSvm(numIterations)
  }
}