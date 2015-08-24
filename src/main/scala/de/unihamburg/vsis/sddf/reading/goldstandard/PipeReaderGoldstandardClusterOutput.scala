package de.unihamburg.vsis.sddf.reading.goldstandard

import java.util.regex.PatternSyntaxException

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import de.unihamburg.vsis.sddf.SddfContext.rddToRdd
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.IdConverter
import de.unihamburg.vsis.sddf.reading.IdConverterBasic
import de.unihamburg.vsis.sddf.reading.SymPair

/**
 * Reads a goldstandard in the following CSV format.
 *
 * ..., clusterId, ..., tupleId, ...
 *               ...
 * ..., clusterId, ..., tupleId, ...
 * 
 * Remark:
 * A output type of RDD[Set[Long]] would be more suitable but the groupByKey() operation
 * returns a Iterable, which is a Seq. For that reason the output type RDD[Seq[Long]] was
 * choosen to avoid costly conversions.
 * 
 */
class PipeReaderGoldstandardClusterOutput(
  separator: Char = ',',
  clusterIdIndex: Int = 0,
  tupleIdIndex: Int = 1,
  idConverter: IdConverter = IdConverterBasic)
  extends PipeElement[RDD[String], RDD[Seq[Long]]] {

  override def step(inputRdd: RDD[String])(implicit pipeContext: AbstractPipeContext): RDD[Seq[Long]] = {
    // parse tuple ids
    val clusterIdTupleIdRdd = inputRdd.map(line => {
      val parts = line.split(separator)
      val tupleId = idConverter.convert(parts(tupleIdIndex).replaceAll("[^0-9]",""))
      val clusterId = idConverter.convert(parts(clusterIdIndex).replaceAll("[^0-9]",""))
      (clusterId, tupleId)
    })
    clusterIdTupleIdRdd.groupByKey().map(_._2.toSeq)
  }

}

object PipeReaderGoldstandardClusterOutput {
  
  def apply(
      separator: Char = ',',
      clusterIdIndex: Int = 0,
      tupleIdIndex: Int = 1,
      idConverter: IdConverter = IdConverterBasic) = {
    new PipeReaderGoldstandardClusterOutput(separator, clusterIdIndex, tupleIdIndex, idConverter)
  }

}