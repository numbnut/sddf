package de.unihamburg.vsis.sddf.logging

import org.slf4j.LoggerFactory

import com.typesafe.scalalogging.slf4j.Logger

trait Logging {

  @transient protected var _log: Logger = null

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (_log == null) {
      _log = Logger(LoggerFactory.getLogger(getClass))
    }
    _log
  }
}
