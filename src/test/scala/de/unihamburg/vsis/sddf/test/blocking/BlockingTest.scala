package de.unihamburg.vsis.sddf.test.blocking

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.indexing.PipeIndexerDummy
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.TupleArray
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.test.util.TestSddfPipeContext

class BlockingTest extends FunSuite with LocalSparkContext with TestSddfPipeContext {

  test("DummyBlocker test") {
    val tuple1: Tuple = new TupleArray(1)
    tuple1.addFeature(0, "test")
    tuple1.id = 1
    val tuple2: Tuple = new TupleArray(1)
    tuple2.addFeature(0, "test2")
    tuple2.id = 2

    val tuples = sc.parallelize(Seq(tuple1, tuple2))

    val dummyIndexer = PipeIndexerDummy()
    val blocks: RDD[SymPair[Tuple]] = dummyIndexer.run(tuples)
    assert(blocks.count === 1)
  }

  // TODO check real Blocker combinations
  //  test("AndBlocker test") {
  //    val tuple1: Tuple = new TupleArrayEriq
  //    tuple1.addFeature(1, "test")
  //    tuple1.id = 1
  //    val tuple2: Tuple = new TupleArrayEriq
  //    tuple1.addFeature(1, "test2")
  //    tuple2.id = 2
  //    val tuple3: Tuple = new TupleArrayEriq
  //    tuple1.addFeature(1, "test3")
  //    tuple2.id = 3
  //
  //    val tuplesLong = sc.parallelize(Seq(tuple1, tuple2, tuple3))
  //
  //    
  //    val dummyBlockers = new DummyBlocker && new DummyBlocker
  //    val blocks: RDD[SymPair[Tuple]] = dummyBlockers.block(tuplesLong)
  //    assert(blocks.count === 3)
  //  }

  //  test("OrBlocker test") {
  //    val tuple1: Tuple = new TupleArrayEriq
  //    tuple1.addFeature(1, "test")
  //    tuple1.id = 1
  //    val tuple2: Tuple = new TupleArrayEriq
  //    tuple1.addFeature(1, "test2")
  //    tuple2.id = 2
  //    val tuple3: Tuple = new TupleArrayEriq
  //    tuple1.addFeature(1, "test3")
  //    tuple2.id = 3
  //
  //    val tuplesShort = sc.parallelize(Seq(tuple1, tuple2))
  //    val tuplesLong = sc.parallelize(Seq(tuple1, tuple2, tuple3))
  //
  //    val dummyBlockers = new DummyBlocker(tuplesShort) || new DummyBlocker(tuplesLong)
  //    val blocks: RDD[SymPair[Tuple]] = dummyBlockers.block()
  //    assert(blocks.count === 3)
  //  }

}
