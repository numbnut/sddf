package de.unihamburg.vsis.sddf.pipe

import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipePassthrough[A, B, C](pipe: Pipe[A, B], pipeInput: A) extends PipeElementPassthrough[C] {
  
  def substep(input: C)(implicit pipeContext: AbstractPipeContext): Unit = {
    pipe.run(pipeInput)
  }
  
}

object PipePassthrough {
  
  def apply[A, B](pipe: Pipe[A, B], pipeInput: A) = new PipePassthrough(pipe, pipeInput)
  
}