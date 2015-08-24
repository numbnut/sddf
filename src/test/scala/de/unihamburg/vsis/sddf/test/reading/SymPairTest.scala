package de.unihamburg.vsis.sddf.test.reading

import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.TupleArray
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext

class SymPairTest extends FunSuite with LocalSparkContext {

  test("test equality and hashcode") {
    val tuple1: Tuple = new TupleArray(1)
    tuple1.addFeature(0, "test")
    tuple1.id = 1
    val tuple2: Tuple = new TupleArray(1)
    tuple1.addFeature(0, "test2")
    tuple2.id = 2
    val pair1 = new SymPair(tuple1, tuple2)
    val pair2 = new SymPair(tuple2, tuple1)

    assert(pair1 === pair1)
    assert(pair2 === pair1)
    assert(pair1 === pair2)
    assert(pair2 === pair2)

    assert(pair1.hashCode() === pair1.hashCode())
    assert(pair2.hashCode() === pair1.hashCode())
    assert(pair1.hashCode() === pair2.hashCode())
    assert(pair2.hashCode() === pair2.hashCode())

    val tuple3: Tuple = new TupleArray(1)
    tuple3.addFeature(0, "test2")
    tuple3.id = 2

    val pair3 = new SymPair(tuple2, tuple3)
    val pair4 = new SymPair(tuple3, tuple2)

    assert(pair3 === pair4)
    assert(!(pair1 === pair3))

    assert(pair3.hashCode() === pair4.hashCode())
    assert(!(pair1.hashCode() === pair3.hashCode()))
  }

  test("test subtraction of RDD[SymPair[Tuple]]") {
    val tuple1 = new TupleArray(1)
    tuple1.id = 1
    val tuple2 = new TupleArray(1)
    tuple2.id = 2
    val pair1 = new SymPair(tuple1, tuple2)
    val pair1swapped = new SymPair(tuple2, tuple1)
    val listPair1 = Seq(pair1)
    val listPair1Swapped = Seq(pair1swapped)
    val list1 = sc.parallelize(listPair1)
    val list1Swapped = sc.parallelize(listPair1Swapped)
    val subtractionResult = list1.subtract(list1Swapped)
    assert(subtractionResult.count() === 0)
  }

}
