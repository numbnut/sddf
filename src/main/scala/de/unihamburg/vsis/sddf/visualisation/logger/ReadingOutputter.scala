package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.ReadingModel

object ReadingOutputter extends Outputter {

  override def logCustomResults(analysable: Analysable): Unit = {
    analysable match {
      case ana: ReadingModel => {
        printLogLine("Number of Tuples", longFormatter(ana.corpusSize))
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the type ReadingModel")
    }
  }

}
