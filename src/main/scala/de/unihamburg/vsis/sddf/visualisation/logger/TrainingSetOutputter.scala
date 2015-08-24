package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.TrainingSetModel

object TrainingSetOutputter extends Outputter {

  override def logCustomResults(analysable: Analysable): Unit = {
    analysable match {
      case ana: TrainingSetModel => {
        printLogLine("Training set size", longFormatter(ana.trainingsSetSize))
        printLogLine("True Positive count", longFormatter(ana.trainingSetTruePostiveCount))
        printLogLine("True Negative count", longFormatter(ana.trainingSetTrueNegativeCount))
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
