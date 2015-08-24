package de.unihamburg.vsis.sddf.visualisation.model

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.SddfContext.Duplicate
import de.unihamburg.vsis.sddf.SddfContext.NoDuplicate


class TrainingSetModel extends BasicAnalysable {

  var _trainingsSetLabeled: Option[RDD[LabeledPoint]] = None

  def trainingsSetLabeled = {
    if (_trainingsSetLabeled.isDefined) {
      _trainingsSetLabeled.get
    } else {
      throw new Exception("Training Set not defined")
    }
  }

  def trainingsSetLabeled_=(trainingsSetLabeled: RDD[LabeledPoint]) = _trainingsSetLabeled = Option(trainingsSetLabeled)

  lazy val trainingsSetSize = trainingsSetLabeled.count()

  lazy val trainingSetTruePostiveCount = {
    val duplicatesFiltered = labelsCounted.filter(_._1 == Duplicate)
    // reducing is invoked on one single entity and is only used for type conversion.
    duplicatesFiltered.map(_._2).reduce(_ + _)
  }

  lazy val trainingSetTrueNegativeCount = {
    val duplicatesFiltered = labelsCounted.filter(_._1 == NoDuplicate)
    // reducing is invoked on one single entity and is only used for type conversion.
    duplicatesFiltered.map(_._2).reduce(_ + _)
  }

  private lazy val labelsCounted = {
    val keyValue = trainingsSetLabeled.map(lPoint => (lPoint.label, 1))
    keyValue.reduceByKey(_ + _)
  }

}
