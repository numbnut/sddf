package de.unihamburg.vsis.sddf.writing

import java.io.File

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeWriterTupleClusterActualDate(folder: String) extends PipeWriterTupleCluster({
  val format = new java.text.SimpleDateFormat("dd-MM-yyyy-HH-mm-ss")
  new File(folder, format.format(new java.util.Date()) + ".csv")
})

object PipeWriterTupleClusterActualDate {
  
  def apply(folder: String) = {
    new PipeWriterTupleClusterActualDate(folder)
  }

}