package de.unihamburg.vsis.sddf.test.reading

import org.scalatest.FunSuite
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.corpus.PipeReaderTupleCsv
import de.unihamburg.vsis.sddf.test.util.TestSddfPipeContext
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.reading.PipeReaderOmitTail

class PipeReaderOmitTailTest
  extends FunSuite
  with LocalSparkContext
  with TestSddfPipeContext {

  test("remove tail") {
    val testInput = sc.parallelize((1 to 100))
    val pipe = PipeReaderOmitTail[Int](10)
    val output = pipe.run(testInput)
    assert(output.count === 90)
  }
  
  test("remove all") {
    val testInput = sc.parallelize((1 to 100))
    val pipe = PipeReaderOmitTail[Int](100)
    val output = pipe.run(testInput)
    assert(output.count === 0)
  }
  
  test("try to remove more than all") {
    val testInput = sc.parallelize((1 to 100))
    val pipe = PipeReaderOmitTail[Int](1000)
    val output = pipe.run(testInput)
    assert(output.count === 0)
  }

}
