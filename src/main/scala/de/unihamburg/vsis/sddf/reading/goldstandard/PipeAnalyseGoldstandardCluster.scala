package de.unihamburg.vsis.sddf.reading.goldstandard

import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.ResultContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.visualisation.model.GoldstandardClusterModel

class PipeAnalyseGoldstandardCluster extends PipeElementPassthrough[RDD[Seq[Long]]] {

  override val _analysable = new GoldstandardClusterModel

  def substep(input: RDD[Seq[Long]])(implicit pipeContext: AbstractPipeContext): Unit = {
    _analysable.goldstandard = input
    pipeContext match {
      case pc: ResultContext => {
        pc.goldstandardModelCluster = Some(_analysable)
      }
      case _ => {
        throw new Exception("Wrong AbstractPipeContext type.")
      }
    }
  }

}

object PipeAnalyseGoldstandardCluster {

  def apply() = new PipeAnalyseGoldstandardCluster()

}