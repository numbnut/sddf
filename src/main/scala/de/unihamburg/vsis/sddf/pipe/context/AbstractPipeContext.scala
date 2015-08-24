package de.unihamburg.vsis.sddf.pipe.context

import de.unihamburg.vsis.sddf.visualisation.ModelRouter

/**
 * Class associated with exactly one pipeline, holding meta information about it.
 */
abstract class AbstractPipeContext(_modelRouter: ModelRouter) {
  
  def modelRouter = _modelRouter
  
}