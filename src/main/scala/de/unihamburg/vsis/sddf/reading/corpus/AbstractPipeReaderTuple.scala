package de.unihamburg.vsis.sddf.reading.corpus

import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.IdConverter
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.TupleArray
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable

abstract class AbstractPipeReaderTuple(
    featureIds: Seq[Int],
    idConverter: IdConverter)
  extends PipeElement[RDD[String], RDD[Tuple]]
  with Serializable {

  @transient override val _analysable = new AlgoAnalysable

  val featureCount = featureIds.max + 1

  /**
   * Parses the line and an Tuple object. Feature names must contain the keyword id because every
   * tuple needs a unique id.
   * Uniqueness will not be checked!
   */
  def parse(line: String): Tuple = {
    val values: Seq[String] = extractValues(line)
    var result = new TupleArray(featureCount)
    (featureIds zip values).foreach(pair => {
      if (pair._1 != FeatureIdNameMapping.Ignore) {
        if (pair._1 == FeatureIdNameMapping.Id) {
          result.id = idConverter.convert(pair._2)
        } else {
          result.addFeature(pair._1, pair._2)
        }
      }
    })
    return result
  }

  override def step(input: RDD[String])(implicit pipeContext: AbstractPipeContext): RDD[Tuple] = {
    input.map(line => {
      this.parse(line)
    })
  }

  /**
   * extract the values in the correct order out of the line
   */
  protected def extractValues(line: String): Seq[String]

}
