package de.unihamburg.vsis.sddf.reading

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeReaderOmitTail[A: ClassTag](lineCount: Long = 1) extends PipeElement[RDD[A], RDD[A]] {

  def step(input: RDD[A])(implicit pipeContext: AbstractPipeContext): RDD[A] = {
    log.debug("Omitting last " + lineCount + " lines.")
    analysable.values += ("lines omitted" -> lineCount.toString())
    val maxIndexRaw = input.count - lineCount
    val maxIndex = if(maxIndexRaw < 0) 0 else maxIndexRaw
    
    input.zipWithIndex().filter(_._2 < maxIndex).map(_._1)
  }

}

object PipeReaderOmitTail {
  
  def apply[A: ClassTag](lineCount: Long = 1) = {
    new PipeReaderOmitTail(lineCount)
  }

}