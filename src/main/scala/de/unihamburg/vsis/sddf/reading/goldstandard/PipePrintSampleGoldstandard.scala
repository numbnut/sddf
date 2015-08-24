package de.unihamburg.vsis.sddf.reading.goldstandard

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.PipeSampler
import de.unihamburg.vsis.sddf.visualisation.Table
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipePrintSampleGoldstandard(count: Int = 10)(implicit fIdNameM: FeatureIdNameMapping)
  extends PipeElementPassthrough[RDD[Tuple]] with PipeSampler {

  def substep(input: RDD[Tuple])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: GoldstandardContext => {
        val sample: Array[SymPair[Tuple]] = pc.goldstandard.takeSample(false, count)
        val table: Seq[Seq[String]] = createSymPairTable(sample)
        
        log.info("Goldstandard sample of " + sample.size + " tuples: ")
        Table.printTable(table)
      }
    }
  }

}

object PipePrintSampleGoldstandard {
  
  def apply(count: Int = 10)(implicit fIdNameM: FeatureIdNameMapping) = {
    new PipePrintSampleGoldstandard(count)
  }

}