package de.unihamburg.vsis.sddf.clustering

import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.ClusterModel
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.pipe.context.ResultContext

class PipeAnalyseClustering extends PipeElementPassthrough[RDD[Set[Tuple]]] {

  override val _analysable = new ClusterModel

  def substep(input: RDD[Set[Tuple]])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: GoldstandardContext with ResultContext => {
        _analysable.clusters = input
        _analysable.goldstandard = pc.goldstandard
        pc.clusterModel = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

object PipeAnalyseClustering {
  
  def apply() = {
    new PipeAnalyseClustering()
  }
  
}
