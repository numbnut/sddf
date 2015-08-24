package de.unihamburg.vsis.sddf.test.evaluation

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.test.util.FixtureHelper
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.visualisation.model.ClusterModel

class ClusterAnalyserTest extends FunSuite with LocalSparkContext with FixtureHelper {
  test("Precission and recall test") {

    val analyser = new ClusterModel
    analyser.clusters = buildClusters()
    analyser.goldstandard = buildGoldstandard()

    assert(analyser.precision === 0.2857142857142857) // should be 2/7
    assert(analyser.recall === 0.6666666666666666) // should be 2/3

  }

  def buildClusters(): RDD[Set[Tuple]] = {
    val cluster1 = initializeTuples(0, 2).toSet
    val cluster2 = initializeTuples(3, 4).toSet
    val cluster3 = initializeTuples(5, 7).toSet

    sc.parallelize(Seq(cluster1, cluster2, cluster3))
  }

  def buildGoldstandard(): RDD[SymPair[Tuple]] = {
    val pair1 = createTuplePair(0, 1)
    val pair2 = createTuplePair(4, 7)
    val pair3 = createTuplePair(6, 7)

    sc.parallelize(Seq(pair1, pair2, pair3))
  }
  
}
