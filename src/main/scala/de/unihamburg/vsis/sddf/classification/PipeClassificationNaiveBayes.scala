package de.unihamburg.vsis.sddf.classification

import scala.beans.BeanInfo
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import org.apache.spark.mllib.classification.NaiveBayesModel

/**
 * Naive Bayes classification PipeElement.
 * TODO this Pipe is actually not working because the Naive Bayes classifier seem to be erroneous.
 * The corresponding test is not working!
 */
class PipeClassificationNaiveBayes(lambda: Double = 1.0) extends AbstractPipeClassification {

  val paramMap: Map[String, Any] = Map(("lambda", lambda))

    def trainModelAndClassify(
    trainingData: RDD[LabeledPoint],
    symPairSim: SymPairSim): RDD[(SymPair[Tuple], Array[Double], Double)] = {
    
    val model = NaiveBayes.train(trainingData, lambda)

    log.debug("Classification Model:" + model)
    log.debug("Classification Model labels :" + model.labels.mkString(" "))
    log.debug("Classification Model pi:     " + model.pi.mkString(" "))
    log.debug("Classification Model theta:  " + model.theta.foreach(_.mkString(" ")))

    // Marking Missing Values as Not Equal (0)
    symPairSim.map(pair => (pair._1, pair._2, model.predict(Vectors.dense(pair._2))))
  }

}

object PipeClassificationNaiveBayes {
  def apply(lambda: Double = 1.0) = {
    new PipeClassificationNaiveBayes(lambda)
  }
}