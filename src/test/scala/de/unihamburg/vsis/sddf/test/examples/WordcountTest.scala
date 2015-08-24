package de.unihamburg.vsis.sddf.test.examples

import org.scalatest.FunSuite
import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.examples.PipeWordcount
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.pipe.context.SddfPipeContext

/**
 * Test wether log calls are only done when the log level is active.
 * This test only works if log level is not set to trace!
 */
class WordcountTest extends FunSuite with LocalSparkContext {

  test("testing wordcount example") {
    val input = sc.textFile("src/test/resources/musicbrainz-1000.csv.dup")
    val wcPipe = PipeWordcount()
    val resultMap = wcPipe.run(input)(new SddfPipeContext())
    val overallWordcount = resultMap.map(_._2).reduce(_ + _)
    assert(8582 === overallWordcount)
  }

}
