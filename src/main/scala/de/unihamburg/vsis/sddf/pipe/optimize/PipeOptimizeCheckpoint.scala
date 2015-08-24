package de.unihamburg.vsis.sddf.pipe.optimize

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeOptimizeCheckpoint extends PipeElementPassthrough[RDD[Any]] {
  
  def substep(input: RDD[Any])(implicit pipeContext: AbstractPipeContext): Unit = {
    input.checkpoint()
  }
}

object PipeOptimizeCheckpoint {
  
  def apply() = new PipeOptimizeCheckpoint()

}