package de.unihamburg.vsis.sddf.visualisation.logger

import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardModel

object GoldstandardOutputter extends Outputter {

  override def logCustomResults(analysable: Analysable) = {
    analysable match {
      case ana: GoldstandardModel => {
        printLogLine("Number of Pairs", longFormatter(ana.goldstandardSize))

        // validation
        if (ana.goldstandardSize < 1) {
          log.error("Goldstandard is Empty! Maybe you picked a wrong file.")
        }
      }
      case _ => log.error("Analysable " + analysable.getClass() +
        " is not of the needed type")
    }
  }

}
