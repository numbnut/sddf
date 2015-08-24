package de.unihamburg.vsis.sddf.test.blocking

import org.apache.spark.rdd.RDD
import org.scalatest.Finders
import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.indexing.PipeIndexerSuffixArray
import de.unihamburg.vsis.sddf.indexing.blocking.PipeBlockerSuffixArray
import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilderBasic
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.TupleArray
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.test.util.TestSddfPipeContext

class SuffixArrayIndexingTest extends FunSuite with LocalSparkContext with TestSddfPipeContext {

  test("testing suffix calculation") {
    val featureId = 0
    implicit val bkvBuilder = new BlockingKeyBuilderBasic((featureId, 0 to 2))

    val tuple1: Tuple = new TupleArray(1)
    tuple1.addFeature(0, "blockingkeyvalue")
    tuple1.id = 1
    val tuples: RDD[Tuple] = sc.parallelize(Seq(tuple1))

    val sab = PipeBlockerSuffixArray(minimumSuffixLength = 4, maximumBlockSize = 12)

    val suffixTuplePairs: Seq[(String, Tuple)] = sab.calcSuffixes(("blockingkeyvalue", tuple1))

    //    println(suffixTuplePairs.map(_._1).mkString("\n"))

    assert(suffixTuplePairs.length === 13)

  }

  test("testing filter blocks") {
    val featureId = 0
    implicit val bkvBuilder = new BlockingKeyBuilderBasic((featureId, 0 to 2))

    val tuple1: Tuple = new TupleArray(1)
    tuple1.addFeature(0, "blockingkeyvalue")
    tuple1.id = 1
    val tuples = sc.parallelize(Seq(tuple1))

    val sab = new PipeBlockerSuffixArray(minimumSuffixLength = 4, maximumBlockSize = 4)

    val suffixTuplePair = ("bla", Seq(tuple1, tuple1, tuple1, tuple1, tuple1))
    assert(sab.filterBlocks(suffixTuplePair) === false)

    val suffixTuplePair2 = ("bla", Seq(tuple1, tuple1, tuple1, tuple1))
    assert(sab.filterBlocks(suffixTuplePair2) === true)

    val suffixTuplePair3 = ("bla", Seq(tuple1))
    assert(sab.filterBlocks(suffixTuplePair3) === false)

    val suffixTuplePair4 = ("bla", Seq(tuple1, tuple1))
    assert(sab.filterBlocks(suffixTuplePair4) === true)
  }

  test("testing whole SAB") {
    val featureId = 0
    implicit val bkvBuilder = new BlockingKeyBuilderBasic((featureId, 0 to 6))

    val tuple1: Tuple = new TupleArray(1)
    tuple1.addFeature(0, "blubluba")
    tuple1.id = 1
    val tuple2: Tuple = new TupleArray(1)
    tuple2.addFeature(0, "blubluba")
    tuple2.id = 2
    val tuple3: Tuple = new TupleArray(1)
    tuple3.addFeature(0, "blubluba")
    tuple3.id = 3
    val tuples = sc.parallelize(Seq(tuple1, tuple2, tuple3))

    val sab = PipeIndexerSuffixArray(minimumSuffixLength = 4, maximumBlockSize = 12)
    val blockingResult: RDD[SymPair[Tuple]] = sab.run(tuples)
    // print(blockingResult.collect().map(symPair => (symPair._1.id, symPair._2.id)).mkString("\n"))
    assert(blockingResult.count === 3)
  }

}
