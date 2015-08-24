package de.unihamburg.vsis.sddf.reading.goldstandard

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.CorpusContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class PipeReaderGoldstandardIdToTuple extends PipeElement[RDD[SymPair[Long]], RDD[SymPair[Tuple]]] {

  def step(input: RDD[SymPair[Long]])(implicit pipeContext: AbstractPipeContext): RDD[SymPair[Tuple]] = {

    pipeContext match {
      case pc: CorpusContext => {
        // prepare id -> tuple mapping
        val tupleIdPairs: RDD[(Long, Tuple)] = pc.corpus.map(t => (t.id, t))

        // (tid1, t) join (tid1, tid2) => (tid1, (tid2, t))
        val firstJoin: RDD[(Long, (Long, Tuple))] = input.map(_.toPair).join(tupleIdPairs)

        val droppedTid1: RDD[(Long, Tuple)] = firstJoin.map(_._2)

        val secondJoin: RDD[(Long, (Tuple, Tuple))] = droppedTid1.join(tupleIdPairs)

        // Drop TupleId and convert to sympair
        secondJoin.map(_._2).map(tuplePair => new SymPair(tuplePair._1, tuplePair._2))
      }
      case _ => { throw new Exception("Wrong AbstractPipeContext Type") }
    }

  }

}

object PipeReaderGoldstandardIdToTuple {
  
  def apply() = new PipeReaderGoldstandardIdToTuple()

}