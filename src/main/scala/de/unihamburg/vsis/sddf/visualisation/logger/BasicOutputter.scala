package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

object BasicOutputter extends Outputter {

  override def logCustomResults(analysable: Analysable) = {
    analysable match {
      case ana: BasicAnalysable => {
        ana.values.foreach(kvp => {
          printLogLine(kvp._1, kvp._2)
        })
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
