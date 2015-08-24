package de.unihamburg.vsis.sddf.similarity

import org.apache.spark.rdd.RDD

import com.rockymadden.stringmetric.StringMetric

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeSimilarity(implicit featureMeasures: Array[(Int, StringMetric[Double])])
  extends PipeElement[RDD[SymPair[Tuple]], RDD[(SymPair[Tuple], Array[Double])]] {

  def step(input: RDD[SymPair[Tuple]])(implicit pipeContext: AbstractPipeContext): RDD[(SymPair[Tuple], Array[Double])] = {
    input.map(
      tuplePair => (tuplePair, SimilarityCalculator.similarity(tuplePair, featureMeasures))
    )
  }

}

object PipeSimilarity {
  
  def apply(implicit featureMeasures: Array[(Int, StringMetric[Double])]) = {
    new PipeSimilarity()
  }

}