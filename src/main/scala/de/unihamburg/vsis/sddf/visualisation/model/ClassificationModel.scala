package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.SddfContext.SymPairSim

class ClassificationModel extends BasicAnalysable {

  var _duplicatePairs: Option[SymPairSim] = None
  def duplicatePairs = {
    if (_duplicatePairs.isDefined) {
      _duplicatePairs.get
    } else {
      throw new Exception("duplicatePairs not defined")
    }
  }
  def duplicatePairs_=(duplicatePairs: SymPairSim) = _duplicatePairs = Option(duplicatePairs)

  lazy val duplicatePairsCount = {
    this.duplicatePairs.count
  }

}
