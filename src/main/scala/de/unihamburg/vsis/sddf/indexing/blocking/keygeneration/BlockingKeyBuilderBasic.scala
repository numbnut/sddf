package de.unihamburg.vsis.sddf.indexing.blocking.keygeneration

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.reading.Tuple

/**
 * Triplets consist of (featureId, Range).
 * The Range prescripes which maximal substring is supposed to be taken of the feature corresponding
 * to the id.
 */
class BlockingKeyBuilderBasic(bkvExtractors: (Int, Range)*)
  extends BlockingKeyBuilder
  with Logging
  with Serializable {

  def buildBlockingKey(tuple: Tuple): String = {
    val result: StringBuilder = new StringBuilder
    bkvExtractors.foreach(triple => {
      val featureId = triple._1
      val range = triple._2
      val featureString = tuple.readFeature(featureId).getOrElse("")
      val feature: Array[Char] = featureString.toCharArray()

      breakable {
        for (i <- range) {
          if (i >= 0) {
            if (i < feature.length) {
              result += feature(i)
            } else {
              break
            }
          }
        }
      }

      if (result.size < 1) {
        log.debug("Check your BKV Extractor Triplet! Feature(id:" + featureId + ") \"" +
          featureString + "\" of tuple " + tuple.id + " cannot be sliced by the range " + range)
      }
    })
    result.toString()
  }

  override val name: String = "BlockingKeyBuilderBasic"

  var paramMapTmp = Map[String, Any]()
  var i = 0
  bkvExtractors.foreach(bkvExtractor => {
    paramMapTmp = paramMapTmp + ("bkvExtractor" + i ->
      ("feature: " + bkvExtractor._1 + ", range: " + rangeToShortString(bkvExtractor._2)))
    i = i + 1
  })
  override val paramMap = paramMapTmp

  private def rangeToShortString(range: Range) = {
    "Range(" + range.min + ", " + range.max + ", " + range.step + ")"
  }

}
