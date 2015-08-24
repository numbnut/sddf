package de.unihamburg.vsis.sddf.indexing

import de.unihamburg.vsis.sddf.convert.PipeConvertClusterToPair
import de.unihamburg.vsis.sddf.filter.PipeFilterDistinct
import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilder
import de.unihamburg.vsis.sddf.indexing.blocking.PipeBlockerSuffixArray

object PipeIndexerSuffixArray {
  
  def apply(minimumSuffixLength: Int = 6, maximumBlockSize: Int = 12)(
    implicit bkvBuilder: BlockingKeyBuilder) = {
    PipeBlockerSuffixArray(minimumSuffixLength, maximumBlockSize)
      .append(PipeConvertClusterToPair())
      .append(PipeFilterDistinct())
  }
  
}