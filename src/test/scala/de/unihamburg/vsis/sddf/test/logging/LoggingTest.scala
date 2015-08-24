package de.unihamburg.vsis.sddf.test.logging

import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.logging.Logging

/**
 * Test wether log calls are only done when the log level is active.
 * This test only works if log level is not set to trace!
 */
class LoggingTest extends FunSuite with Logging {

  var longRun = false

  test("testing function call") {
    log.trace(longRunningFunction())
    assert(longRun === false)
    log.error(longRunningFunction())
    assert(longRun === true)
  }

  private def longRunningFunction(): String = {
    longRun = true
    "Logging Test is doing some test output <- can be ignored"
  }

}
