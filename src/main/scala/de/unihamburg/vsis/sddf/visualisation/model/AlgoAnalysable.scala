package de.unihamburg.vsis.sddf.visualisation.model

import de.unihamburg.vsis.sddf.Parameterized

class AlgoAnalysable extends BasicAnalysable {

  var _algo: Option[Parameterized] = None
  def algo: Option[Parameterized] = _algo
  def algo_=(algo: Parameterized) = _algo = Some(algo)
  
}