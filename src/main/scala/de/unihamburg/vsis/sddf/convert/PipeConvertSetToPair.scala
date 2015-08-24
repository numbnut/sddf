package de.unihamburg.vsis.sddf.convert

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.SddfContext._
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.SymPair

/**
 * Computes the connected components and returns them.
 */
class PipeConvertSetToPair[A] extends PipeElement[RDD[Set[A]], RDD[SymPair[A]]] {
  
  def step(input: RDD[Set[A]])(implicit pipeContext: AbstractPipeContext): RDD[SymPair[A]] = {
    val sequence = input.map(_.toSeq)
    sequence.cartesianBlocksWithoutIdentity()
  }
  
}

object PipeConvertSetToPair {
  
  def apply[A]() = new PipeConvertSetToPair[A]()

}