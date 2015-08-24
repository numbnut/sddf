package de.unihamburg.vsis.sddf.test.util

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.TupleArray

trait FixtureHelper {
  def initializeTuples(idFrom: Int = 0, idTo: Int): Seq[Tuple] = {
    var result: Seq[Tuple] = Seq()
    for (i <- idFrom to idTo) {
      val tuple = new TupleArray(2)
      tuple.id = i
      result = tuple +: result
    }
    result
  }

  def createTuplePair(id1: Int, id2: Int): SymPair[Tuple] = {
    val tuple1 = new TupleArray(2)
    tuple1.id = id1
    val tuple2 = new TupleArray(2)
    tuple2.id = id2
    new SymPair(tuple1, tuple2)
  }
}
