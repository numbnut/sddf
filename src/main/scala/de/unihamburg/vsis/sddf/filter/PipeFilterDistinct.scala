package de.unihamburg.vsis.sddf.filter

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext

class PipeFilterDistinct[A] extends PipeElement[RDD[A], RDD[A]] {

  def step(input: RDD[A])(implicit pipeContext: AbstractPipeContext): RDD[A] = {
    input.distinct()
  }

}

object PipeFilterDistinct {
  def apply[A]() = {
    new PipeFilterDistinct[A]()
  }
}