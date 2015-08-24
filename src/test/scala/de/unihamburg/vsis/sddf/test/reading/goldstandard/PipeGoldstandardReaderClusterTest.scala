package de.unihamburg.vsis.sddf.test.reading.goldstandard

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeReaderGoldstandardIdToTuple
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeReaderGoldstandardIdsCluster
import de.unihamburg.vsis.sddf.test.util.FixtureHelper
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.test.util.TestSddfPipeContext

class PipeReaderGoldstandardClusterTest
  extends FunSuite
  with LocalSparkContext
  with TestSddfPipeContext
  with FixtureHelper {

  test("test goldstandard tuple reading in cluster format") {
    // format clusterId, tupleId
    val input: RDD[String] = sc.parallelize(Seq("1,1", "2,2", "2,3"))
    val gsReaderPipe = PipeReaderGoldstandardIdsCluster()
    gsReaderPipe.start(input)
    val gsIds = gsReaderPipe.output.get
    assert(gsIds.count() === 1)

    val tuples: Seq[Tuple] = initializeTuples(1, 3)
    pc.corpus = sc.parallelize(tuples)
    val gsconverterPipe = new PipeReaderGoldstandardIdToTuple
    gsconverterPipe.start(gsIds)
    val gsTuple = gsconverterPipe.output.get
    assert(gsTuple.count() === 1)
  }

  test("test goldstandard id reading in cluster format") {
    // format clusterId, tupleId
    val input: RDD[String] = sc.parallelize(Seq("1,1", "2,2", "2,3"))
    val gsReaderPipe = PipeReaderGoldstandardIdsCluster()
    gsReaderPipe.start(input)
    val result = gsReaderPipe.output.get
    assert(result.count() === 1)
  }

  test("test goldstandard cluster reader from file") {
    val input = sc.textFile("src/test/resources/musicbrainz-1000.csv.dup")
    val gsReaderPipe = PipeReaderGoldstandardIdsCluster()
    gsReaderPipe.start(input)
    val result = gsReaderPipe.output.get
    assert(result.collect().size === 13)
  }

}