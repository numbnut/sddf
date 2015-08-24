package de.unihamburg.vsis.sddf.pipe.optimize

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.SddfPipeContext

class PipeOptimizePersistAndName[A](rddname: String = null, newLevel: StorageLevel = StorageLevel.MEMORY_ONLY) extends PipeElementPassthrough[RDD[A]] {
  
  def substep(input: RDD[A])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: SddfPipeContext => {
        input.persist(newLevel)
        if(rddname != null){
          input.name = rddname
          pc.persistedRDDs += (rddname -> input)
          analysable.values += ("name" -> rddname)
        }
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }
}

object PipeOptimizePersistAndName {
  
  def apply[A](rddname: String = null, newLevel: StorageLevel = StorageLevel.MEMORY_ONLY) = {
    new PipeOptimizePersistAndName[A](rddname, newLevel)
  }

}