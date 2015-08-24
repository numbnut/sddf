package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class IndexingModel extends BasicAnalysable {

  var _pairs: Option[RDD[SymPair[Tuple]]] = None
  def pairs = {
    if (_pairs.isDefined) {
      _pairs.get
    } else {
      throw new Exception("Pairs not defined")
    }
  }
  def pairs_=(pairs: RDD[SymPair[Tuple]]) = _pairs = Option(pairs)

  var _corpus: Option[RDD[Tuple]] = None
  def corpus = {
    if (_corpus.isDefined) {
      _corpus.get
    } else {
      throw new Exception("Corpus not defined")
    }
  }
  def corpus_=(corpus: RDD[Tuple]) = _corpus = Option(corpus)

  lazy val reducedSearchSpace = pairs.count

  lazy val naiveSearchSpace = {
    val tupleCount = corpus.count
    ((tupleCount * tupleCount) - tupleCount) / 2;
  }

  lazy val searchSpaceReductionRatio: Double = {
    1 - (reducedSearchSpace / naiveSearchSpace.toDouble)
  }
  
  lazy val searchSpaceReductionMagnitude: Double = {
    var factor: Double = (naiveSearchSpace.toDouble / reducedSearchSpace)
    var result = 0.0
    while(factor > 1){
      factor = factor / 10.0
      if(factor <= 1){ 
        result = result + factor
      }else{
        result = result + 1
      }
    }
    result
  }

}
