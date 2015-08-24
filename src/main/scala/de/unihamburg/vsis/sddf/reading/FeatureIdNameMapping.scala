package de.unihamburg.vsis.sddf.reading

import de.unihamburg.vsis.sddf.logging.Logging

class FeatureIdNameMapping(featureMapping: Map[Int, String]) extends Logging with Serializable {

  val inverseFeatureMapping = featureMapping.map(_.swap)

  def lookupFeatureName(featureId: Int): String = {
    val featureNameOption = featureMapping.get(featureId)
    if (!featureNameOption.isEmpty) {
      return featureNameOption.get
    }
    log.error("Feature name for feature id " + featureId + " is not defined")
    "###FeatureNameNotPresent###"
  }

  def lookupFeatureId(featureName: String): Int = {
    val featureNameOption = inverseFeatureMapping.get(featureName)
    if (!featureNameOption.isEmpty) {
      return featureNameOption.get
    }
    log.error("Feature id for feature name " + featureName + " is not defined")
    FeatureIdNameMapping.Ignore
  }
}

/**
 * companion object
 */
object FeatureIdNameMapping {

  val Id = -1

  val Ignore = -2

}
