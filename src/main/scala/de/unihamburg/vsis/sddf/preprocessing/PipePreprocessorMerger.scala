package de.unihamburg.vsis.sddf.preprocessing

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipePreprocessorMerger(featA: Int, featB: Int, newFeatId: Int, separator: String = " ")
  extends TraitPipePreprocessor
  with Serializable
  with Logging {
  
  def clean(tuple: Tuple): Tuple = {
    val attrAvalueOption = tuple.readFeature(featA)
    val attrBvalueOption = tuple.readFeature(featB)
    if (!attrAvalueOption.isEmpty && !attrBvalueOption.isEmpty) {
      tuple.addFeature(newFeatId, attrAvalueOption.get + separator + attrBvalueOption.get)
    } else {
      log.warn("Feature " + featA + " or " + featB + " empty in tuple " + tuple)
    }
    tuple
  }
  
}

object PipePreprocessorMerger {
  
  def apply(featA: Int, featB: Int, newFeatId: Int, separator: String = " ") = {
    new PipePreprocessorMerger(featA, featB, newFeatId, separator)
  }

}