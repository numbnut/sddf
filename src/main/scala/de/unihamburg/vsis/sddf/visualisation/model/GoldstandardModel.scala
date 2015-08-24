package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class GoldstandardModel extends BasicAnalysable {

  var _goldstandard: Option[RDD[SymPair[Tuple]]] = None
  def goldstandard = _goldstandard
  def goldstandard_=(goldstandard: RDD[SymPair[Tuple]]) = _goldstandard = Option(goldstandard)

  lazy val goldstandardSize = {
    if (goldstandard.isDefined) {
      goldstandard.get.count()
    } else {
      throw new Exception("No goldstandard present")
    }
  }

}
