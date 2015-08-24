package de.unihamburg.vsis.sddf.pipe

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeFilterRDD[A](filter: A => Boolean) extends PipeElement[RDD[A], RDD[A]] {
  
  def step(input: RDD[A])(implicit pipeContext: AbstractPipeContext): RDD[A] = {
    input.filter(filter)
  }
  
}

object PipeFilterRDD {
  
  def apply[A](filter: A => Boolean) = {
    new PipeFilterRDD(filter)
  }
  
}