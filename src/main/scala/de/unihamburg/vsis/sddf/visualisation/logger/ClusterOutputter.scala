package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.ClusterModel

/**
 * TODO need caching for relevant RDDs
 */
object ClusterOutputter extends Outputter {

  def logCustomResults(analysable: Analysable) = {
    analysable match {
      case ana: ClusterModel => {
        printLogLine("Recall", doubleFormatter(ana.recall))
        printLogLine("Precision", doubleFormatter(ana.precision))
        this.printLightSeparator()
        printLogLine("Number of clusters", longFormatter(ana.clusterCount))
        printLogLine("Number of duplicates", longFormatter(ana.duplicateCount))
        printLogLine("Number of duplicate pairs", longFormatter(ana.duplicatePairCount))
        printLogLine("Average cluster size", doubleFormatter(ana.averageClusterSize))
        log.debug("Cluster size distribution (size, count): " + 
        		ana.clusterSizeDistribution.mkString("\n"))
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
