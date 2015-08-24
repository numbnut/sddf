package de.unihamburg.vsis.sddf.sparkextensions

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.reading.SymPair

class RDDSequenceFunctions[A <: Any](val rdd: RDD[Seq[A]]) {
  
  /**
   * Creates the cartesian product of every sequence with itself.
   * Identity tuples like (a,a) are omitted.
   * RDD(Seq(1,2,3)) returns RDD((1,2), (1,3), (2,3))
   */
  def cartesianBlocksWithoutIdentity(): RDD[SymPair[A]] = {
    rdd.flatMap(block => {
      val maxIndex = block.length - 1
      for (a <- 0 to maxIndex; b <- a + 1 to maxIndex) yield new SymPair(block(a), block(b))
    })
  }
  
}
