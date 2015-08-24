package de.unihamburg.vsis.sddf.pipe.context

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

trait GoldstandardContext {

  var _goldstandard: RDD[SymPair[Tuple]] = null
  def goldstandard = _goldstandard
  def goldstandard_=(goldstandard: RDD[SymPair[Tuple]]) = {
    goldstandard.name = "Goldstandard"
    _goldstandard = goldstandard
  }

}