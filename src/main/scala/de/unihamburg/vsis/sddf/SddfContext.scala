package de.unihamburg.vsis.sddf

import org.apache.spark.rdd.RDD

import com.rockymadden.stringmetric.StringMetric

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.sparkextensions.RDDSequenceFunctions
import de.unihamburg.vsis.sddf.tuple.TupleSymPairFunctions

object SddfContext {

  // Typedefinitions
  type SymPairSim = RDD[(SymPair[Tuple], Array[Double])]
  type FeatureId = Int
  type Threshold = Double
  type Measure = StringMetric[Double]
  type Similarity = Double
  
  // Constants
  val NoDuplicate = 0D
  val Duplicate = 1D
  
  // implicit conversions
  implicit def symPairToTupleSymPairFunctions[A <: Tuple](pair: SymPair[A]) = {
    new TupleSymPairFunctions(pair)
  }

  /**
   * TODO: check, maybe dangerous to do this
   */
  implicit def symPairToTuple[A <: Tuple](pair: SymPair[A]): Tuple2[A, A] = {
    (pair._1, pair._2)
  }
  
  implicit def rddToRdd[A <: Any](rdd: RDD[Seq[A]]) = new RDDSequenceFunctions(rdd)
  
  // more convinient typing of the idNameMappings
  implicit def pairToInt(pair: (Int, String)) = pair._1

}
