package de.unihamburg.vsis.sddf.pipe

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext

/**
 * A pipe is a single step in a pipeline with a predecessor and a successor.
 * Only linear pipelines are supported.
 */
trait Pipe[A, B] extends Logging {

  val name = this.getClass.getSimpleName

  var _input: Option[A] = None
  def input = _input
  def input_=(input: A) = this._input = Some(input)

  var _output: Option[B] = None
  def output = _output
  def output_=(output: B) = this._output = Some(output)

  def step(input: A)(implicit pipeContext: AbstractPipeContext): B

  def start(input: A)(implicit pipeContext: AbstractPipeContext): Unit
  
  def start(implicit pipeContext: AbstractPipeContext): Unit = {
    if(this.input.isDefined){
      start(this.input.get)
    }else{
      log.warn("No input defined for pipe " + name + ".")
    }
  }
  
   /**
   * Start the pipe and return the result
   */
  def run(input: A)(implicit pipeContext: AbstractPipeContext): B
  
  def append[D](pipe: Pipe[B, D]): Pipeline[A, D]

  def prepend[D](pipe: Pipe[D, A]): Pipeline[D, B]
}