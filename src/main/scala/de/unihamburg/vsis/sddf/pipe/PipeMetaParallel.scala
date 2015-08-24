package de.unihamburg.vsis.sddf.pipe

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

/**
 * Contains multiple Pipes which run on the same input set.
 * Output is merged via RDD method union and passed to the next step.
 */
class PipeMetaParallel[I, O](pipes: Pipe[RDD[I], RDD[O]]*) extends PipeElement[RDD[I], RDD[O]] {

  override def step(input: RDD[I])(implicit pipeContext: AbstractPipeContext): RDD[O] = {
    pipes.map(pipe => {
      pipe.run(input)
    }).reduce(_ union _)
  }

  override def start(input: RDD[I])(implicit pipeContext: AbstractPipeContext): Unit = {
    analysable.name = this.name
    analysable.startTime = new DateTime
    this.output = this.step(input)
    analysable.endTime = new DateTime
    if (nextStep.isDefined) {
      nextStep.get.start(this.output.get)
    }
  }

}

object PipeMetaParallel {
  
  def apply[I, O](pipes: Pipe[RDD[I], RDD[O]]*) = new PipeMetaParallel(pipes: _*)

}