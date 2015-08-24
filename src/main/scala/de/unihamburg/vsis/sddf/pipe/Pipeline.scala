package de.unihamburg.vsis.sddf.pipe

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext

/**
 *
 */
class Pipeline[A, C](
    protected val startPipe: PipeElement[A, _],
    protected val endPipe: PipeElement[_, C])
  extends Pipe[A, C]
  with Logging {

  def this(firstPipe: PipeElement[A, C]) = {
    this(firstPipe, firstPipe)
  }

  def step(input: A)(implicit pipeContext: AbstractPipeContext): C = {
    startPipe.start(input)
    val result = endPipe.output
    if (result.isDefined) {
      result.get
    } else {
      // should never be reached!
      throw new Exception("No result computed by pipeline. This should not happen.")
    }
  }

  /**
   * Start the pipe without returning the result.
   */
  override def start(input: A)(implicit pipeContext: AbstractPipeContext): Unit = {
      this.step(input)
  }
  
  /**
   * Start the pipe and return the result
   */
  override def run(input: A)(implicit pipeContext: AbstractPipeContext): C = {
    start(input)
    this.output.get
  }
  
  /**
   * Start the pipe and return the result
   */
  def run(implicit pipeContext: AbstractPipeContext): C = {
    if(this.input.isDefined){
    	start(pipeContext)
    	this.output.get
   }else{
     throw new Exception("No input defined for pipeline.")
   }
  }

  override def append[D](pipe: Pipe[C, D]): Pipeline[A, D] = {
    pipe match {
      case p: PipeElement[C, D] => {
        endPipe.nextStep = p
        p.prevStep = endPipe
        new Pipeline(startPipe, p)
      }
      case p: Pipeline[C, D] => {
        this.endPipe.nextStep = p.startPipe
        p.startPipe.prevStep = this.endPipe
        new Pipeline(startPipe, p.endPipe)
      }
      case _ => throw new Exception("Unknown Pipe implementation!")
    }
  }

  override def prepend[D](pipe: Pipe[D, A]): Pipeline[D, C] = {
    pipe match {
      case p: PipeElement[D, A] => {
        startPipe.prevStep = p
        p.nextStep = startPipe
        new Pipeline(p, endPipe)
      }
      case p: Pipeline[D, A] => {
        this.startPipe.prevStep = p.endPipe
        p.endPipe.nextStep = this.startPipe
        new Pipeline(p.startPipe, endPipe)
      }
      case _ => throw new Exception("Unknown Pipe implementation!")
    }
  }

  override def output() = endPipe.output

  override def input() = startPipe.input
  override def input_=(input: A) = startPipe.input = input

}