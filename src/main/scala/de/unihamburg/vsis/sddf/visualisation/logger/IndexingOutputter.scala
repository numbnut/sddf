package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModel

object IndexingOutputter extends Outputter {

  override def logCustomResults(analysable: Analysable) = {
    analysable match {
      case ana: IndexingModel => {
        printLogLine("Reduced search space size", longFormatter(ana.reducedSearchSpace))
        printLogLine("Naive search space size", longFormatter(ana.naiveSearchSpace))
        printLogLine("Reduction ratio", doubleFormatter(ana.searchSpaceReductionRatio))
        printLogLine("Search space reduction magnitude", doubleFormatter(ana.searchSpaceReductionMagnitude))
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
