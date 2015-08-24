package de.unihamburg.vsis.sddf.preprocessing

import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipePreprocessorToLower(featureId: Int*) extends TraitPipePreprocessor with Serializable {

  def clean(tuple: Tuple): Tuple = {
    featureId.foreach(fId => {
      // transform the given features
      tuple.applyOnFeature(fId, _.toLowerCase())
    })
    tuple
  }

}

object PipePreprocessorToLower {
  
  def apply(featureId: Int*) = new PipePreprocessorToLower(featureId: _*)

}