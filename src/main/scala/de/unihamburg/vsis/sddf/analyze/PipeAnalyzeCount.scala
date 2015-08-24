package de.unihamburg.vsis.sddf.analyze

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext

class PipeAnalyzeCount[A] extends PipeElementPassthrough[RDD[A]] {
  
  def substep(input: RDD[A])(implicit pipeContext: AbstractPipeContext): Unit = {
    analysable.values += ("Element count" -> input.count().toString)
  }
  
}

object PipeAnalyzeCount {
  def apply[A]() = {
    new PipeAnalyzeCount[A]()
  }
}