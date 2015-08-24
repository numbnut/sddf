package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.IndexingModel
import de.unihamburg.vsis.sddf.visualisation.model.BlockingModel

object BlockingOutputter extends Outputter {

  override def logCustomResults(analysable: Analysable) = {
    analysable match {
      case ana: BlockingModel => {
        printLogLine("Block count", longFormatter(ana.blockCount))
        printLogLine("Average block size", doubleFormatter(ana.averageBlockSize))
        printLogLine("Block size distribution", "")
        ana.blockDistribution.foreach(pair => {
        	printLogLine(pair._1.toString, longFormatter(pair._2))
        })
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
