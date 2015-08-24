package de.unihamburg.vsis.sddf.visualisation.model

class BasicAnalysable extends Analysable {
  /**
   * Map to store arbitrary key value pairs in which will be send to output.
   */
  val values: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
}