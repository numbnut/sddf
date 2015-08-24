package de.unihamburg.vsis.sddf.classification

import scala.compat.Platform

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import com.rockymadden.stringmetric.StringMetric

import de.unihamburg.vsis.sddf.SddfContext.Duplicate
import de.unihamburg.vsis.sddf.SddfContext.NoDuplicate
import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.similarity.SimilarityCalculator
import de.unihamburg.vsis.sddf.sparkextensions.RddUtils.securlyZipRdds
import de.unihamburg.vsis.sddf.visualisation.model.TrainingSetModel
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeClassificationTrainingDataGenerator(
  truePositiveCount: Int = 500,
  trueNegativeCount: Int = 500)(
  implicit featureMeasures: Array[(Int, StringMetric[Double])])
  extends PipeElement[SymPairSim, (SymPairSim, RDD[LabeledPoint])]
  with Logging {

  override def step(input: SymPairSim)(implicit pipeContext: AbstractPipeContext) = {
    pipeContext match {
      case pc: GoldstandardContext with CorpusContext => {
        var truePositiveFraction = truePositiveCount / pc.goldstandard.count.toDouble
        var trueNegativeFraction = trueNegativeCount / pc.corpus.count.toDouble
        log.debug("True positive pair fraction taken from the gold standard for training purposes: " + truePositiveFraction)
        log.debug("True negative pair fraction taken from the corpus for training purposes: " + trueNegativeFraction)
        if (truePositiveFraction > 1.0) {
          truePositiveFraction = 1.0
          log.debug("True positive pair fraction limited to 1.0")
        }
        if (trueNegativeFraction > 1.0) {
          trueNegativeFraction = 1.0
          log.debug("True negative pair fraction limited to 1.0")
        }
        val result = generateTrainingData(pc.corpus, pc.goldstandard,
          truePositiveFraction, trueNegativeFraction)
        (input, result)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

  /**
   * TODO harden against false input and huge values greater than the available amount of tuples
   * TODO optimize takeSample returns an Array which means to ship all the data to the driver node
   * that slows processing down. It would be nice to have way of sampling directly into an RDD.
   * I couldn't find a solution for now.
   */
  def generateTrainingData(
    corpus: RDD[Tuple],
    goldstandard: RDD[SymPair[Tuple]],
    truePositiveFraction: Double,
    trueNegativeFraction: Double): RDD[LabeledPoint] = {

    // true postive / goldstandard sample
    val truePositiveSampleRdd = goldstandard.sample(false, truePositiveFraction)
    log.debug("goldstandard.count: " + goldstandard.count)
    log.debug("truePositiveFraction: " + truePositiveFraction)
    log.debug("truePositiveSampleRdd.count: " + truePositiveSampleRdd.count)
    val goldstandardSimilarityRdd: RDD[Array[Double]] = truePositiveSampleRdd.map(
      tuplePair => SimilarityCalculator.similarity(tuplePair, featureMeasures)
    )
    val goldstandardLabeledPointsRdd = goldstandardSimilarityRdd.map(
      array => LabeledPoint(label = Duplicate, features = Vectors.dense(array))
    )

    // true negative sample
    val trueNegativesRdd = sampleTrueNegatives(corpus, goldstandard, trueNegativeFraction)

    val trueNegativesSimilarityRDD: RDD[Array[Double]] = trueNegativesRdd.map(
      tuplePair => SimilarityCalculator.similarity(tuplePair, featureMeasures)
    )
    val trueNegativesLabeledPointsRdd = trueNegativesSimilarityRDD.map(
      array => LabeledPoint(label = NoDuplicate, features = Vectors.dense(array))
    )

    trueNegativesLabeledPointsRdd union goldstandardLabeledPointsRdd

  }

  /**
   * The challenge is to build a true negative set without pairs contained in the goldstandard.
   */
  private def sampleTrueNegatives(
    corpus: RDD[Tuple],
    goldstandard: RDD[SymPair[Tuple]],
    trueNegativeFraction: Double): RDD[SymPair[Tuple]] = {
    var doubledTrueNegativeFraction = trueNegativeFraction * 2
    if (doubledTrueNegativeFraction > 1.0) {
      doubledTrueNegativeFraction = 1.0
    }
    val trueNegativeSample: RDD[Tuple] = corpus.sample(false, doubledTrueNegativeFraction, Platform.currentTime)
    val trueNegativeSampleSize = trueNegativeSample.count()
    val split = trueNegativeSample.randomSplit(Array(0.5, 0.5))
    val trueNeg1: RDD[Tuple] = split(0)
    val trueNeg2: RDD[Tuple] = split(1)

    val trueNegativePairs: RDD[(Tuple, Tuple)] = securlyZipRdds(trueNeg1, trueNeg2)
    val trueNegativesRdd: RDD[SymPair[Tuple]] = trueNegativePairs.map(pair => new SymPair(pair._1, pair._2))
    val prevSize = trueNegativesRdd.count
    var cleanedTrueNegativesRdd = trueNegativesRdd.subtract(goldstandard)
    val tuplesMissing = cleanedTrueNegativesRdd.count - prevSize
    if (tuplesMissing > 0) {
      log.debug(tuplesMissing + " tuples where removed from the true negative set, because they " +
        "are already contained in the goldstandard. Try to recursively add some random tuples.")
      val mssingTrueNeg = sampleTrueNegatives(corpus, goldstandard, tuplesMissing.toInt)
      cleanedTrueNegativesRdd = cleanedTrueNegativesRdd union mssingTrueNeg
    }
    cleanedTrueNegativesRdd
  }
}

/**
 * companion object
 */
object PipeClassificationTrainingDataGenerator {

  val All = -1
  
  def apply(
      truePositiveCount: Int = 500,
      trueNegativeCount: Int = 500)(
      implicit featureMeasures: Array[(Int, StringMetric[Double])]) = {
    new PipeClassificationTrainingDataGenerator(truePositiveCount, trueNegativeCount)
  }

}
