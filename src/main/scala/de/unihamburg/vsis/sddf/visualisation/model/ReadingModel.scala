package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.Tuple

class ReadingModel extends BasicAnalysable {

  var _tuples: Option[RDD[Tuple]] = None
  def tuples = {
    if (_tuples.isDefined) {
      _tuples.get
    } else {
      throw new Exception("Tuples not defined")
    }
  }
  def tuples_=(tuples: RDD[Tuple]) = _tuples = Option(tuples)

  lazy val corpusSize = tuples.count()

}
