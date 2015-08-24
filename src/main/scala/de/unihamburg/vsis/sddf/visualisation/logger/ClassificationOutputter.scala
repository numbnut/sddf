package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.ClassificationModel

object ClassificationOutputter extends Outputter {

  override def logCustomResults(analysable: Analysable): Unit = {
    analysable match {
      case ana: ClassificationModel => {
        printLogLine("Number of duplicate pairs found", longFormatter(ana.duplicatePairsCount))
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
