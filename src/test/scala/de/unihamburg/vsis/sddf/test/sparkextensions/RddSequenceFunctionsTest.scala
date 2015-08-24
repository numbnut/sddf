package de.unihamburg.vsis.sddf.test.sparkextensions

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import de.unihamburg.vsis.sddf.SddfContext.rddToRdd
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import org.scalatest.Matchers

class RddSequenceFunctionsTest extends FunSuite with LocalSparkContext with Matchers {

  test("cartesian product on every block") {
    val testRdd: RDD[Seq[Int]] = sc.parallelize(Seq(Seq(1, 2, 3), Seq(3), Seq()))
    val result: Set[SymPair[Int]] = testRdd.cartesianBlocksWithoutIdentity.collect().toSet
    val expected = Set(new SymPair(1, 2), new SymPair(1, 3), new SymPair(2, 3))
    result should contain theSameElementsAs expected
  }

}
