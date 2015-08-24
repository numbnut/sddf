package de.unihamburg.vsis.sddf.reading

import java.lang.Long

import de.unihamburg.vsis.sddf.logging.Logging
/**
 * This implementation is inteded to be used for tuples with a dense feature vector.
 */
class TupleArray(size: Int) extends Tuple with Logging {
  
  def this(features: String*) = {
    this(features.size)
    featureArray = features.toArray
  }
  
  def this(size: Int, features: String*) = {
    this(size)
    featureArray = features.toArray
  }

  var featureArray = new Array[String](size)
  
  def id_=(value: String): Unit = _id = Long.parseLong(value)

  def getOriginalId: String = {
    Long.toString(_id).toUpperCase()
  }

  def iterator(): Iterator[(Int, String)] = {
    featureArray.zipWithIndex.map(_.swap).iterator
  }
  
  def addFeature(featureId: Int, value: String) = {
    if (indexValid(featureId)) {
      if (!FeaturePresent(featureId)) {
        featureArray(featureId) = value
      }
    }
  }

  def updateFeature(featureId: Int, value: String): Option[String] = {
    if (indexValidAndFeaturePresent(featureId)) {
      val ret = featureArray(featureId)
      featureArray(featureId) = value
      Some(ret)
    } else {
      None
    }
  }

  def addOrUpdateFeature(featureId: Int, value: String): Option[String] = {
    if (indexValid(featureId)) {
      val oldValue = updateFeature(featureId, value)
      if (oldValue == None) {
        addFeature(featureId, value)
      }
      oldValue
    } else {
      None
    }
  }

  def removeFeature(featureId: Int): Option[String] = {
    if (indexValidAndFeaturePresent(featureId)) {
      val oldValue = featureArray(featureId)
      featureArray(featureId) = null
      Some(oldValue)
    } else {
      None
    }
  }

  def readFeature(featureId: Int): Option[String] = {
    if (indexValidAndFeaturePresent(featureId)) {
      Some(featureArray(featureId))
    } else {
      None
    }
  }

  def applyOnFeature(featureId: Int, transform: String => String): Tuple = {
    if (indexValidAndFeaturePresent(featureId)) {
      featureArray(featureId) = transform(featureArray(featureId))
    }
    this
  }

  private def indexValid(index: Int): Boolean = {
    if (index >= featureArray.length || index < 0) {
      log.warn("Feature Id " + index + " out of Range 0-" + size + " of Tuple: " + this)
      false
    } else {
      true
    }
  }

  private def FeaturePresent(index: Int): Boolean = {
    if (featureArray(index) == null) {
      false
    } else {
      true
    }
  }

  private def indexValidAndFeaturePresent(index: Int): Boolean = {
    if (indexValid(index) && FeaturePresent(index)) {
      true
    } else {
      false
    }
  }

}
