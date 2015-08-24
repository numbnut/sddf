package de.unihamburg.vsis.sddf.visualisation

import de.unihamburg.vsis.sddf.visualisation.model.Analysable

trait ModelRouter {

  def submitModel(model: Analysable): Unit

}