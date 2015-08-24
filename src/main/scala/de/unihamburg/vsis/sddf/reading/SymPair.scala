package de.unihamburg.vsis.sddf.reading

/**
 * Represents a symmetric pair. (a,b) == (b,a) && (a,b) == (a,b)
 * TODO it would be nice to extend the Tuple2 case class of scala to inherit their functionality, 
 * but extending case classes is a bad smell.
 */
class SymPair[A](a: A, b: A) extends Equals with Serializable {

  def _1 = a

  def _2 = b

  def this(pair: (A, A)) = {
    this(pair._1, pair._2)
  }

  def toPair = (_1, _2)

  override def equals(other: Any) = {
    other match {
      case that: SymPair[A] =>
        (that canEqual this) &&
          ((this._1 == that._1 && this._2 == that._2) || (this._1 == that._2 && this._2 == that._1))
      case _ => false
    }
  }

  def canEqual(other: Any): Boolean = {
    other.isInstanceOf[SymPair[A]]
  }

  override def hashCode: Int = {
    41 * ((41 + a.hashCode()) + (41 + b.hashCode()))
  }

  override def toString = {
    "SymPair(" + a + ", " + b + ")"
  }

}
