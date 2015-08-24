package de.unihamburg.vsis.sddf.indexing

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.indexing.blocking.PipeBlockerSortedNeighborhood
import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilder
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

object PipeIndexerSortedNeighborhood {
  
  def apply(windowSize: Int = 10)(implicit bkvBuilder: BlockingKeyBuilder) = {
    PipeBlockerSortedNeighborhood(windowSize)
    .append(SortedNeighborhoodIndexer())
  }
  
}

/**
 * Helper Class to do the pair generation.
 */
class SortedNeighborhoodIndexer extends PipeElement[RDD[Seq[Tuple]], RDD[SymPair[Tuple]]] {
  
  def step(input: RDD[Seq[Tuple]])(implicit pipeContext: AbstractPipeContext): RDD[SymPair[Tuple]] = {
    import de.unihamburg.vsis.sddf.indexing.SortedNeighborhoodIndexer._
     input.zipWithIndex.flatMap(pair => {
      val index = pair._2
      val window = pair._1
      if (index == 0) {
        cartesianBlocksWithoutIdentity(window)
      } else {
        pairsWithLastElement(window)
      }
    })
  }
  
}

object SortedNeighborhoodIndexer {
  def apply() = {
    new SortedNeighborhoodIndexer()
  }
  
  def cartesianBlocksWithoutIdentity[A](seq: Seq[A]): Seq[SymPair[A]] = {
    for (a <- 0 until seq.length; b <- a + 1 until seq.length) yield new SymPair(seq(a), seq(b))
  }

  def pairsWithLastElement[A](seq: Seq[A]): Seq[SymPair[A]] = {
    val last = seq.last
    for (i <- 0 until seq.length - 1) yield new SymPair(seq(i), last)
  }

  /**
   * calculates the number of pairs a sorted neighbourhood run will result in.
   */
  def calcPairCount(elementCount: Int, windowSize: Int): Int = {
    val windowCount = elementCount - windowSize + 1
    val firstWindowPairs = (windowSize * (windowSize - 1)) / 2
    val lastWindowPairs = (windowCount - 1) * (windowSize - 1)
    firstWindowPairs + lastWindowPairs
  }
}