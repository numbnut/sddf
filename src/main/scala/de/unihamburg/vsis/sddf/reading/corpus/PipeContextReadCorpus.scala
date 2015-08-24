package de.unihamburg.vsis.sddf.reading.corpus

import org.apache.spark.rdd.RDD
import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.pipe.PipeElement
import scala.reflect.ClassTag

class PipeContextReadCorpus[A: ClassTag] extends PipeElement[RDD[A], RDD[Tuple]] {

  def step(input: RDD[A])(implicit pipeContext: AbstractPipeContext): RDD[Tuple] = {
    pipeContext match {
      case pc: CorpusContext => pc.corpus
    }
  }
}

object PipeContextReadCorpus {

  def apply[A]() = new PipeContextReadCorpus()

}