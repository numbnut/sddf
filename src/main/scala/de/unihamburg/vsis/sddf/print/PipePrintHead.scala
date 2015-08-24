package de.unihamburg.vsis.sddf.print

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext

class PipePrintHead[A](count: Int = 10) extends PipeElementPassthrough[RDD[A]] {

  def substep(input: RDD[A])(implicit pipeContext: AbstractPipeContext): Unit = {
    val toPrint = input.take(count)
    toPrint.foreach(x => {
      println(x)
    })
  }

}

object PipePrintHead {
  
  def apply[A](count: Int = 10) = new PipePrintHead[A](count)
  
}