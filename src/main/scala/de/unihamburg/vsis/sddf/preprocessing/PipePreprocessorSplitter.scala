package de.unihamburg.vsis.sddf.preprocessing

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipePreprocessorSplitter(featureId: Int, splitRegex: String, newfeatureIds: Int*)
  extends TraitPipePreprocessor
  with Serializable
  with Logging {

  def clean(tuple: Tuple): Tuple = {
    val attrValueOption = tuple.readFeature(featureId)
    if (!attrValueOption.isEmpty) {
      val parts = attrValueOption.get.split(splitRegex)
      if (parts.length == newfeatureIds.length) {
        parts.zip(newfeatureIds).foreach(
          partNamePair => { tuple.addFeature(partNamePair._2, partNamePair._1) }
        )
      } else {
        log.warn("Splitting tuple failed. Number of parts " + parts.length + " != " +
          newfeatureIds.length + " number of features (" + newfeatureIds + "). Tuple: " + tuple)
      }
    } else {
      log.warn("Feature " + featureId + " empty in tuple " + tuple)
    }
    tuple
  }

}

object PipePreprocessorSplitter {
  
  def apply(featureId: Int, splitRegex: String, newfeatureIds: Int*) = {
    new PipePreprocessorSplitter(featureId, splitRegex, newfeatureIds: _*)
  }

}