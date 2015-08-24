package de.unihamburg.vsis.sddf.classification

import org.apache.spark.rdd.RDD

import com.rockymadden.stringmetric.StringMetric

import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class PipePrintSampleFalseNegatives(
    count: Int = 10)(
    implicit featureIdNameMapping: FeatureIdNameMapping,
    featureMeasures: Array[(Int, StringMetric[Double])])
  extends AbstractPipePrintFalseTuples(count) {

  def selectFalseTuples(goldstandard: RDD[SymPair[Tuple]], input: RDD[SymPair[Tuple]]) = {
    goldstandard.subtract(input)
  }

  def filterFalseTuplesForOutput(falseTuplesWithSimilarity: RDD[(SymPair[Tuple], Array[Double])]) = {
    falseTuplesWithSimilarity.takeSample(false, count)
  }

  def logMessage(count: Int): String = {
    "Sampling " + count + " false negatives. (duplicate pairs which are no duplicates)"
  }

}

object PipePrintSampleFalseNegatives {
  
  def apply(
    count: Int = 10)(
    implicit featureIdNameMapping: FeatureIdNameMapping, 
    featureMeasures: Array[(Int, StringMetric[Double])]) = {
    new PipePrintSampleFalseNegatives(count)
  }

}