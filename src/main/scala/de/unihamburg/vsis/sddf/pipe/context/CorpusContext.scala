package de.unihamburg.vsis.sddf.pipe.context

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.Tuple

trait CorpusContext {

  var _corpus: RDD[Tuple] = null
  def corpus = _corpus
  def corpus_=(corpus: RDD[Tuple]) = {
    corpus.name = "Corpus"
    _corpus = corpus
  }

}