package de.unihamburg.vsis.sddf.preprocessing

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.Tuple

trait TraitPipePreprocessor extends PipeElement[RDD[Tuple], RDD[Tuple]] {

  def clean(tuple: Tuple): Tuple

  def step(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): RDD[Tuple] = {
    input.map(clean(_))
  }

}
