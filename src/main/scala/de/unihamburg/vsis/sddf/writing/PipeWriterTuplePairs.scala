package de.unihamburg.vsis.sddf.writing

import java.io.File
import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeWriterTuplePairs(file: File, separator: Char = ',') extends PipeElementPassthrough[RDD[SymPair[Tuple]]] {

  def substep(input: RDD[SymPair[Tuple]])(implicit pipeContext: AbstractPipeContext): Unit = {
    val writer = new TupleWriterFile(file, separator)
    val collected = input.collect()
    collected.foreach(pair => {
      writer.writeTuple(pair._1)
      writer.writeTuple(pair._2)
      writer.blankLine()
    })
    writer.close()
  }

}

object PipeWriterTuplePairs {
  
  def apply(file: File, separator: Char = ',') = {
    new PipeWriterTuplePairs(file, separator)
  }

}