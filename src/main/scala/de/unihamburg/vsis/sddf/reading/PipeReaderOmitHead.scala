package de.unihamburg.vsis.sddf.reading

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

/**
 * TODO improve performance by doing something like
 * rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
 */
class PipeReaderOmitHead[A: ClassTag](lineCount: Long = 1) extends PipeElement[RDD[A], RDD[A]] {

  def step(input: RDD[A])(implicit pipeContext: AbstractPipeContext): RDD[A] = {
    log.debug("Omitting first " + lineCount + " lines.")
    analysable.values += ("lines omitted" -> lineCount.toString())
    input.zipWithIndex().filter(_._2 >= lineCount).map(_._1)
  }

}

object PipeReaderOmitHead {
  
  def apply[A: ClassTag](lineCount: Long = 1) = {
    new PipeReaderOmitHead(lineCount)
  }

}