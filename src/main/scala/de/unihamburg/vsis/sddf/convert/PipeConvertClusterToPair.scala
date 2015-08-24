package de.unihamburg.vsis.sddf.convert

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.SddfContext.rddToRdd
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.SymPair

/**
 * Converts a 
 */
class PipeConvertClusterToPair[A]() extends PipeElement[RDD[Seq[A]], RDD[SymPair[A]]] {

  override def step(inputRdd: RDD[Seq[A]])(implicit pipeContext: AbstractPipeContext): RDD[SymPair[A]] = {
    inputRdd.cartesianBlocksWithoutIdentity
  }

}

object PipeConvertClusterToPair {
  
  def apply[A]() = {
    new PipeConvertClusterToPair[A]()
  }

}