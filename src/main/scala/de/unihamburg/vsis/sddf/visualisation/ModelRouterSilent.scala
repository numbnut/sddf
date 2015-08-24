package de.unihamburg.vsis.sddf.visualisation

import de.unihamburg.vsis.sddf.visualisation.model.Analysable

/**
 * This class does exactly nothing and is the default model sink.
 */
object ModelRouterSilent extends ModelRouter {
  def submitModel(model: Analysable): Unit = {}
}