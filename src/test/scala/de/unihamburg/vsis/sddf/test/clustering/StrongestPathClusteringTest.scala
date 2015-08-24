package de.unihamburg.vsis.sddf.test.clustering

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.clustering.PipeClusteringStrongestPath
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.test.util.FixtureHelper
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.test.util.TestSddfPipeContext

class StrongestPathClusteringTest
  extends FunSuite
  with LocalSparkContext
  with TestSddfPipeContext
  with FixtureHelper {

  test("simple cluster test") {
    val pair1 = (createTuplePair(1, 2), Array(0.4, 0.6))
    val pair2 = (createTuplePair(2, 4), Array(0.1, 0.2))
    val pair3 = (createTuplePair(4, 3), Array(0.6, 0.8))
    val pair4 = (createTuplePair(3, 1), Array(0.0, 0.2))

    val pairs: RDD[(SymPair[Tuple], Array[Double])] = sc.parallelize(Seq(pair1, pair2, pair3, pair4))
    val clusterer = new PipeClusteringStrongestPath
    clusterer.start(pairs)
    val clusterResult: Array[Set[Tuple]] = clusterer.output.get.collect()

    val expectedResult = Array(Set(pair1._1._1, pair1._1._2), Set(pair3._1._1, pair3._1._2))
    assert(clusterResult === expectedResult)
  }

}
