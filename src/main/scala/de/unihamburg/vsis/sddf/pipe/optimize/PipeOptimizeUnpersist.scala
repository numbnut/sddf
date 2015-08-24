package de.unihamburg.vsis.sddf.pipe.optimize

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.SddfPipeContext

class PipeOptimizeUnpersist[A](rddname: String) extends PipeElementPassthrough[RDD[A]] {

  def substep(input: RDD[A])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: SddfPipeContext => {
        val rddOption = pc.persistedRDDs.get(rddname)
        if (rddOption.isDefined) {
          rddOption.get.unpersist()
          analysable.values += ("RDD unpersisted" -> rddname)
        } else {
          log.warn("Can't unpersist RDD with the name " + rddname)
        }
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }
}

object PipeOptimizeUnpersist {

  def apply[A](rddname: String) = {
    new PipeOptimizeUnpersist[A](rddname)
  }

}