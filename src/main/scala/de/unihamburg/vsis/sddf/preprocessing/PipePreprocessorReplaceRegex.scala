package de.unihamburg.vsis.sddf.preprocessing

import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable

class PipePreprocessorReplaceRegex(regex: String, replacement: String, featureId: Int*)
  extends TraitPipePreprocessor
  with Serializable
  with Parameterized {
  
  @transient override val _analysable = new AlgoAnalysable
  _analysable.algo = this
  override val paramMap = Map("regex" -> regex, "replacement" -> replacement) ++ featureId.map(("featureId", _)).toMap

  def clean(tuple: Tuple): Tuple = {
    featureId.foreach(fId => {
      // transform the given features
      tuple.applyOnFeature(fId, _.replaceAll(regex, replacement))
    })
    tuple
  }

}

object PipePreprocessorReplaceRegex {

  def apply(regex: String, replacement: String, featureId: Int*) = {
    new PipePreprocessorReplaceRegex(regex, replacement, featureId: _*)
  }

}