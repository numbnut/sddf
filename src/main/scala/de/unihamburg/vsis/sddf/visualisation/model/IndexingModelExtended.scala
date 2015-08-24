package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class IndexingModelExtended extends IndexingModel with RecallAndPrecision {

  var _goldstandard: Option[RDD[SymPair[Tuple]]] = None
  def goldstandard = {
    if (_goldstandard.isDefined) {
      _goldstandard.get
    } else {
      throw new Exception("Goldstandard not defined")
    }
  }
  def goldstandard_=(goldstandard: RDD[SymPair[Tuple]]) = _goldstandard = Option(goldstandard)

  
  lazy val rssRecall: Double = {
    recall(goldstandard, pairs)
  }
  
  lazy val rssPrecision: Double = {
    precision(goldstandard, pairs)
  }

}
