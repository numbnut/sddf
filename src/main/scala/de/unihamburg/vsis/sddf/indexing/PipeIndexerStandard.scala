package de.unihamburg.vsis.sddf.indexing

import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilder
import de.unihamburg.vsis.sddf.indexing.blocking.PipeBlockerStandard
import de.unihamburg.vsis.sddf.convert.PipeConvertClusterToPair

object PipeIndexerStandard {
  
  def apply(implicit bkvBuilder: BlockingKeyBuilder) = {
    PipeBlockerStandard(bkvBuilder)
      .append(PipeConvertClusterToPair())
  }
  
}