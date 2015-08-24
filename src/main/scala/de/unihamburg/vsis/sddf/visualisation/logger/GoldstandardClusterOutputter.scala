package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardModel
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardClusterModel

object GoldstandardClusterOutputter extends Outputter {

  override def logCustomResults(analysable: Analysable) = {
    analysable match {
      case ana: GoldstandardClusterModel => {
    	  printLogLine("Duplicate Count", longFormatter(ana.duplicateCount))
    	  printLogLine("Cluster Distribution", "")
        ana.goldstandardClusterDistribution.foreach(entry => {
        	printLogLine(entry._1.toString, longFormatter(entry._2))
        })
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
