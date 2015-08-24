package de.unihamburg.vsis.sddf.reading.goldstandard

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.IdConverter
import de.unihamburg.vsis.sddf.reading.IdConverterBasic
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

/**
 * Reads a Goldstandard in CSV pair format e.g.
 * ..., <tupleId>, ..., <tupleId>, ...
 *  ...
 * ..., <tupleId>, ..., <tupleId>, ...
 */
class PipeReaderGoldstandardIdsPairs(
    separator: Char = ',',
    idIndex1: Int = 0,
    idIndex2: Int = 1,
    idConverter: IdConverter = IdConverterBasic)
  extends PipeElement[RDD[String], RDD[SymPair[Long]]] {

  override def step(inputRdd: RDD[String])(implicit pipeContext: AbstractPipeContext): RDD[SymPair[Long]] = {
    inputRdd.map(line => {
      val parts = line.split(separator)
      val tupleId1 = idConverter.convert(parts(idIndex1).replaceAll("[^0-9]",""))
      val tupleId2 = idConverter.convert(parts(idIndex2).replaceAll("[^0-9]",""))
      new SymPair(tupleId1, tupleId2)
    })
  }

}

object PipeReaderGoldstandardIdsPairs {
  
  def apply(
      separator: Char = ',',
      idIndex1: Int = 0,
      idIndex2: Int = 1,
      idConverter: IdConverter = IdConverterBasic) = {
    new PipeReaderGoldstandardIdsPairs(separator, idIndex1, idIndex2, idConverter)
  }

}