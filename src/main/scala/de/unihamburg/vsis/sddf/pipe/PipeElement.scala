package de.unihamburg.vsis.sddf.pipe

import org.joda.time.DateTime

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

trait PipeElement[A, B] extends Pipe[A, B] with Logging with Serializable {

  @transient var _prevStep: Option[Pipe[_, A]] = None
  def prevStep = _prevStep
  def prevStep_=(prevStep: Pipe[_, A]) = this._prevStep = Option(prevStep)

  @transient var _nextStep: Option[Pipe[B, _]] = None
  def nextStep = _nextStep
  def nextStep_=(nextStep: Pipe[B, _]) = this._nextStep = Option(nextStep)

  @transient val _analysable: BasicAnalysable = new BasicAnalysable
  def analysable = _analysable

  override def start(input: A)(implicit pipeContext: AbstractPipeContext): Unit = {
    analysable.name = this.name
    analysable.startTime = new DateTime
    this.output = step(input)
    analysable.endTime = new DateTime
    pipeContext.modelRouter.submitModel(analysable)
    if (nextStep.isDefined) {
      nextStep.get.start(this.output.get)
    }
  }
  
   /**
   * Start the pipe and return the result
   */
  override def run(input: A)(implicit pipeContext: AbstractPipeContext): B = {
    start(input)
    this.output.get
  }

  override def append[D](pipe: Pipe[B, D]): Pipeline[A, D] = {
    pipe match {
      case p: PipeElement[B, D] => {
        this.nextStep = pipe
        p.prevStep = this
        new Pipeline(this, p)
      }
      case p: Pipeline[B, D] => p.prepend(this)
      case _                 => throw new Exception("Unknown Pipe implementation!")
    }
  }

  override def prepend[D](pipe: Pipe[D, A]): Pipeline[D, B] = {
    pipe match {
      case p: PipeElement[D, A] => {
        this.prevStep = pipe
        p.nextStep = this
        new Pipeline(p, this)
      }
      case p: Pipeline[D, A] => p.append(this)
      case _                 => throw new Exception("Unknown Pipe implementation!")
    }

  }

}