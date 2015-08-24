package de.unihamburg.vsis.sddf.preprocessing

import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable
import de.unihamburg.vsis.sddf.Parameterized

class PipePreprocessorRemoveRegex(regex: String, featureId: Int*) 
  extends PipePreprocessorReplaceRegex(regex, "", featureId: _*)

object PipePreprocessorRemoveRegex {
  
  def apply(regex: String, featureId: Int*) = {
    new PipePreprocessorRemoveRegex(regex, featureId: _*)
  }

}