package de.unihamburg.vsis.sddf.reading.goldstandard

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import de.unihamburg.vsis.sddf.SddfContext.rddToRdd
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.IdConverter
import de.unihamburg.vsis.sddf.reading.IdConverterBasic
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.convert.PipeConvertClusterToPair

/**
 * Reads a goldstandard in the following CSV format.
 *
 * ..., clusterId, ..., tupleId, ...
 *               ...
 * ..., clusterId, ..., tupleId, ...
 * 
 */

object PipeReaderGoldstandardIdsCluster {
  
  def apply(
      separator: Char = ',',
      clusterIdIndex: Int = 0,
      tupleIdIndex: Int = 1,
      idConverter: IdConverter = IdConverterBasic) = {
    PipeReaderGoldstandardClusterOutput(separator, clusterIdIndex, tupleIdIndex, idConverter)
    .append(PipeConvertClusterToPair())
  }

}