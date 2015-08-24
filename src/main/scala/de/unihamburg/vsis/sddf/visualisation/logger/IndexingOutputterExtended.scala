package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModelExtended

object IndexingOutputterExtended extends Outputter {

  override def logCustomResults(analysable: Analysable) = {
    analysable match {
      case ana: IndexingModelExtended => {
        printLogLine("Reduced search space size", longFormatter(ana.reducedSearchSpace))
        printLogLine("Naive search space size", longFormatter(ana.naiveSearchSpace))
        printLogLine("Reduction ratio", doubleFormatter(ana.searchSpaceReductionRatio))
        printLogLine("Search space reduction magnitude", doubleFormatter(ana.searchSpaceReductionMagnitude))
        printLogLine("RSS Recall", doubleFormatter(ana.rssRecall))
        printLogLine("RSS Precision", doubleFormatter(ana.rssPrecision))
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
