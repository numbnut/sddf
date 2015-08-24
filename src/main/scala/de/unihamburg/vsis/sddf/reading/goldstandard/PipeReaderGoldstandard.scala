package de.unihamburg.vsis.sddf.reading.goldstandard

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.Pipeline
import de.unihamburg.vsis.sddf.reading.IdConverter
import de.unihamburg.vsis.sddf.reading.IdConverterBasic
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

object PipeReaderGoldstandardPairs {

  def apply(
    separator: Char = ',',
    idIndex1: Int = 0,
    idIndex2: Int = 1,
    idConverter: IdConverter = IdConverterBasic): Pipeline[RDD[String], RDD[SymPair[Tuple]]] = {
    PipeReaderGoldstandardIdsPairs(separator, idIndex1, idIndex2, idConverter)
      .append(PipeReaderGoldstandardIdToTuple())
  }

}

object PipeReaderGoldstandardCluster {

  def apply(
      separator: Char = ',',
      clusterIdIndex: Int = 0,
      tupleIdIndex: Int = 1,
      idConverter: IdConverter = IdConverterBasic): Pipeline[RDD[String], RDD[SymPair[Tuple]]] = {
    PipeReaderGoldstandardIdsCluster(separator, clusterIdIndex, tupleIdIndex, idConverter)
      .append(PipeReaderGoldstandardIdToTuple())
  }

}