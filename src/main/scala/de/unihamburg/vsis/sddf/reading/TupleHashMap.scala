package de.unihamburg.vsis.sddf.reading

import de.unihamburg.vsis.sddf.logging.Logging

/**
 * This implementation is inteded to be used for sparse features.
 * Tuple represents a single tuple with a unique id.
 * Features are stored in a map.
 * TODO check whether equals is implemented correctly.
 * Also check out the Equals trait and decide whether we should use it.
 * TODO optimize: use more memory efficient datastructure
 */
abstract class TupleHashMap extends Tuple with Logging {

  /**
   * The map containing all Feature value mappings
   */
  val map = scala.collection.mutable.HashMap[Int, String]()

  def iterator(): Iterator[(Int, String)] = {
    map.iterator
  }
  
  override def addFeature(featureId: Int, value: String) = {
    if (!map.contains(featureId)) {
      map += featureId -> value
    } else {
      log.warn("Feature " + featureId + " already present in tuple " + this)
    }
  }

  override def updateFeature(featureId: Int, value: String): Option[String] = {
    if (map.contains(featureId)) {
      val oldValue = map.get(featureId)
      map += featureId -> value
      return oldValue
    }
    return None
  }

  def addOrUpdateFeature(featureId: Int, value: String): Option[String] = {
    if (map.contains(featureId)) {
      return updateFeature(featureId, value)
    } else {
      addFeature(featureId, value)
      return None
    }
  }

  override def removeFeature(featureId: Int): Option[String] = {
    map.remove(featureId)
  }

  override def readFeature(featureId: Int): Option[String] = {
    map.get(featureId)
  }

  override def applyOnFeature(featureId: Int, transform: String => String): Tuple = {
    val FeatureOption = map.get(featureId)
    if (FeatureOption.isDefined) {
      val FeatureValue = FeatureOption.get
      val newFeatureValue = transform(FeatureValue)
      map.update(featureId, newFeatureValue)
    }
    return this
  }

  override def equals(other: Any) = {
    other match {
      case that: de.unihamburg.vsis.sddf.reading.TupleHashMap => this.id == that.id
      case _ => false
    }
  }

  override def hashCode() = {
    id.hashCode()
  }

}
