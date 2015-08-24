package de.unihamburg.vsis.sddf.reading

abstract class Tuple() extends Serializable with Iterable[(Int, String)]{
  // TODO add: with Ordered[Tuple] {

  /**
   * unique id of this tuple
   */
  var _id: Long = -1

  def id = _id

  def id_=(value: Long): Unit = _id = value

  /**
   * Adds and only adds a new Feature if the name does not already exist
   */
  def addFeature(featureId: Int, value: String)

  /**
   * Applies add Feature on each name, value pair
   */
  def addFeatures(featureIds: Seq[Int], values: Seq[String]) = {
    (featureIds zip values).foreach(nvPair => addFeature(nvPair._1, nvPair._2))
  }

  /**
   * Updates an Feature and returns the old value
   */
  def updateFeature(featureId: Int, value: String): Option[String]

  /**
   * Updates an Feature if the name exists or adds a new one if not
   */
  def addOrUpdateFeature(featureId: Int, value: String): Option[String]

  /**
   * removes an Feature and returens the removed value
   */
  def removeFeature(featureId: Int): Option[String]

  /**
   * returns the Features value
   */
  def readFeature(featureId: Int): Option[String]

  /**
   * apply function on the target Feature value and return the tuple itself
   */
  def applyOnFeature(featureId: Int, transform: String => String): Tuple

  /**
   * Basic performant equals method.
   * Returns true if other is an Instance of Tuple and the ids are equal
   * It does not compare the Features!
   */
  override def equals(other: Any) = {
    other match {
      case that: Tuple =>
        (that canEqual this) &&
          id == that.id
      case _ => false
    }
  }

  override def canEqual(other: Any): Boolean = {
    other.isInstanceOf[Tuple]
  }

  override def hashCode: Int = {
    41 * (41 + id.hashCode())
  }

  override def toString(): String = {
    "id -> " + id
  }

  def toStringVerbose(implicit featureIdNameMapping: FeatureIdNameMapping): String = {
    var result = "id -> " + id + ",  "
    this.foreach(keyVal => {
      result += featureIdNameMapping.lookupFeatureName(keyVal._1) + " -> " + keyVal._2 + ",\t"
    })
    result
  }

  /**
   * comparison is delegated to the ids
   */
  //  def compare(that: Tuple): Int = {
  //    this.id compare that.id
  //  }

}

object Tuple{
  
  def apply(size: Int) = {
    new TupleArray(size)
  }
  
  def apply(features: String*) = {
    new TupleArray(features: _*)
  }
    
  def apply(size: Int, features: String*) = {
    new TupleArray(size, features: _*)
  }
  
}
