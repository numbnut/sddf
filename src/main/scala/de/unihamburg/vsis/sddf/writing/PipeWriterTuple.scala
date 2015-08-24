package de.unihamburg.vsis.sddf.writing

import java.io.File

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeWriterTuple(file: File, separator: Char = ',')
  extends PipeElementPassthrough[RDD[Tuple]]
  with Parameterized {

  def substep(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): Unit = {
    val writer = new TupleWriterFile(file, separator)
    writer.writeTuple(input)
  }

  val paramMap: Map[String, Any] = Map(("file", file), ("separator", separator))

}

object PipeWriterTuple {

  def apply(file: File, separator: Char = ',') = {
    new PipeWriterTuple(file, separator)
  }

}