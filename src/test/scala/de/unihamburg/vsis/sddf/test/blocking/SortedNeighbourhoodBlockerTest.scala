package de.unihamburg.vsis.sddf.test.blocking

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.Matchers
import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilderBasic
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.TupleArray
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.test.util.TestSddfPipeContext
import de.unihamburg.vsis.sddf.indexing.PipeIndexerSortedNeighborhood
import de.unihamburg.vsis.sddf.indexing.PipeIndexerSortedNeighborhood

class SortedNeighborhoodIndexingTest
  extends FunSuite
  with LocalSparkContext
  with TestSddfPipeContext
  with Matchers {

  test("testing whole Sorted Neighborhood Indexer") {
    val featureId = 1
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
    val tuple4: Tuple = new TupleArray(1)
    tuple4.addFeature(0, "blubluba")
    tuple4.id = 4
    val tuple5: Tuple = new TupleArray(1)
    tuple5.addFeature(0, "blubluba")
    tuple5.id = 5
    val tuples = sc.parallelize(Seq(tuple1, tuple2, tuple3, tuple4, tuple5))

    val indexer = PipeIndexerSortedNeighborhood(windowSize = 3)
    val blockingResult: RDD[SymPair[Tuple]] = indexer.run(tuples)
    assert(blockingResult.count === 7)

    val resultArray = blockingResult.collect()
    resultArray.foreach(println(_))
    val expectedResult = Seq(
      new SymPair(tuple1, tuple2), new SymPair(tuple1, tuple3), new SymPair(tuple2, tuple3), new SymPair(tuple2, tuple4), new SymPair(tuple3, tuple4), new SymPair(tuple3, tuple5), new SymPair(tuple4, tuple5)
    )

    resultArray should contain theSameElementsAs expectedResult
  }

}
