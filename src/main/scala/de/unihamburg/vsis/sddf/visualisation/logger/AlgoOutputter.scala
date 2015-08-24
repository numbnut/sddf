package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable
import de.unihamburg.vsis.sddf.visualisation.model.Analysable

object AlgoOutputter extends Outputter {
  def logCustomResults(analysable: Analysable): Unit = {
    analysable match {
      case ana: AlgoAnalysable => {
//        printLogLine("Algo", ana.algo.get.algoName)
        printMap(ana.algo.get.paramMap)
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }

  }
}