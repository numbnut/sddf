package de.unihamburg.vsis.sddf.classification

import org.apache.spark.rdd.RDD

import com.rockymadden.stringmetric.StringMetric

import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class PipePrintSampleFalsePositives(
    count: Int = 10)(
    implicit featureIdNameMapping: FeatureIdNameMapping,
    featureMeasures: Array[(Int, StringMetric[Double])])
  extends AbstractPipePrintFalseTuples(count) {
  
  def selectFalseTuples(goldstandard: RDD[SymPair[Tuple]], input: RDD[SymPair[Tuple]]) = {
    input.subtract(goldstandard)
  }

  def filterFalseTuplesForOutput(falseTuplesWithSimilarity: RDD[(SymPair[Tuple], Array[Double])]) = {
    falseTuplesWithSimilarity.takeSample(false, count)
  }
  
  def logMessage(count: Int): String = {
    "Sampling " + count + " false positives. (duplicate pairs which were not found)"
  }

}

object PipePrintSampleFalsePositives {
  
  def apply(
    count: Int = 10)(
    implicit featureIdNameMapping: FeatureIdNameMapping, 
    featureMeasures: Array[(Int, StringMetric[Double])]) = {
    new PipePrintSampleFalsePositives(count)
  }

}