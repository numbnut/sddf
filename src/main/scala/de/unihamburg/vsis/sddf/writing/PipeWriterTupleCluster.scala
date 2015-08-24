package de.unihamburg.vsis.sddf.writing

import java.io.File

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeWriterTupleCluster(file: File, separator: Char = ',')
  extends PipeElementPassthrough[RDD[Set[Tuple]]] {

  def substep(input: RDD[Set[Tuple]])(implicit pipeContext: AbstractPipeContext): Unit = {
    val writer = new TupleWriterFile(file, separator)
    // TODO write tuples to hdfs in parallel and merge them afterwards
    val collected = input.collect()
    collected.foreach(set => {
      set.foreach(tuple => {
        writer.writeTuple(tuple)
      })
      writer.blankLine()
    })
    writer.close()
  }

}

object PipeWriterTupleCluster {

  def apply(file: File, separator: Char = ',') = {
    new PipeWriterTupleCluster(file, separator)
  }

}