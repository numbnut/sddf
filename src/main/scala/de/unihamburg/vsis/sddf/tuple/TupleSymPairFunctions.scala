package de.unihamburg.vsis.sddf.tuple

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

class TupleSymPairFunctions[A <: Tuple](pair: SymPair[A]) {

  def pairId: SymPair[Long] = new SymPair[Long](pair._1.id, pair._2.id)

}
