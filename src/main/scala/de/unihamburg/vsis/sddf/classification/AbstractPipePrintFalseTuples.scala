package de.unihamburg.vsis.sddf.classification

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.rockymadden.stringmetric.StringMetric

import de.unihamburg.vsis.sddf.pipe.PipeElementPassthrough
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.pipe.context.GoldstandardContext
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.PipeSampler
import de.unihamburg.vsis.sddf.visualisation.Table
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

abstract class AbstractPipePrintFalseTuples(
  count: Int)(
    implicit featureIdNameMapping: FeatureIdNameMapping,
    featureMeasures: Array[(Int, StringMetric[Double])])
  extends PipeElementPassthrough[RDD[(SymPair[Tuple], Array[Double])]]
  with PipeSampler {

  def selectFalseTuples(goldstandard: RDD[SymPair[Tuple]], input: RDD[SymPair[Tuple]]): RDD[SymPair[Tuple]]

  def filterFalseTuplesForOutput(falseTuplesWithSimilarity: RDD[(SymPair[Tuple], Array[Double])]): Array[(SymPair[Tuple], Array[Double])]

  def logMessage(count: Int): String

  def substep(input: RDD[(SymPair[Tuple], Array[Double])])(implicit pipeContext: AbstractPipeContext): Unit = {
    pipeContext match {
      case pc: GoldstandardContext => {

        val falseTuples = selectFalseTuples(pc.goldstandard, input.map(_._1))

        if (falseTuples.count > 0) {
          val dummyValue: RDD[(SymPair[Tuple], Int)] = falseTuples.map((_, 1))
          val join: RDD[(SymPair[Tuple], (Int, Option[Array[Double]]))] = dummyValue.leftOuterJoin(input)
          val falsePositivesWithSimilarity: RDD[(SymPair[Tuple], Array[Double])] = join.map(pair => {
            (pair._1, pair._2._2.getOrElse(Array()))
          })

          val falseTuplesSample = filterFalseTuplesForOutput(falsePositivesWithSimilarity)

          val table = createSymPairSimVectorTable(falseTuplesSample)
          log.info(logMessage(count))
          Table.printTable(table)
        } else {
          log.info(logMessage(0))
        }
      }
    }
  }

}