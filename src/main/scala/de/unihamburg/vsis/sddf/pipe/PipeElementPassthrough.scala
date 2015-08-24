package de.unihamburg.vsis.sddf.pipe

import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext

abstract class PipeElementPassthrough[A] extends PipeElement[A, A] {
  
  def substep(input: A)(implicit pipeContext: AbstractPipeContext): Unit
  
  def step(input: A)(implicit pipeContext: AbstractPipeContext): A = {
    substep(input: A)
    input
  }
  
}