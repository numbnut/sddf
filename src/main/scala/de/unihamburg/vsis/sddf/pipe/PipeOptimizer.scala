package de.unihamburg.vsis.sddf.pipe

import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
/**
 * Pass a measure and multiple Pipes to the PipeOptimizer.
 * It will execute all of them and will forward the best result regarding the measure.
 * The first result with the highest measure value will be forwarded.
 */
class PipeOptimizer[A, B](measure: B => Double, pipes: Pipe[A, B]*) extends PipeElement[A, B] {
  def step(input: A)(implicit pipeContext: AbstractPipeContext): B = {
    var bestResultScore = Double.MinValue;
    var result: Option[B] = None
    pipes.foreach(pipe => {
      val newResult = pipe.step(input)
      val newResultScore = measure(newResult)
      if (newResultScore > bestResultScore) {
        result = Some(newResult)
        bestResultScore = newResultScore
      }
    })
    if (result.isDefined) {
      result.get
    } else {
      throw new Exception("No PipeElements passed to the PipeOptimizer.")
    }
  }
}