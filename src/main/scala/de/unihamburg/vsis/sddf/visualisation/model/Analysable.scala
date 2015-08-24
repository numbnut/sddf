package de.unihamburg.vsis.sddf.visualisation.model

import org.joda.time.DateTime

trait Analysable {
  
  var _name: String = "unnamed"
  def name = _name
  def name_=(name: String) = _name = name
  
  var _startTime: DateTime = new DateTime
  def startTime = _startTime
  def startTime_=(startTime: DateTime) = _startTime = startTime
  
  var _endTime: DateTime = new DateTime
  def endTime = _endTime
  def endTime_=(endTime: DateTime) = _endTime = endTime
  
}