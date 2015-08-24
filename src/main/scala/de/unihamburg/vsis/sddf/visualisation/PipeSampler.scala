package de.unihamburg.vsis.sddf.visualisation

import com.rockymadden.stringmetric.StringMetric

import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple

trait PipeSampler {

  def createSymPairTable(symPairs: Seq[SymPair[Tuple]])(implicit fIdNameM: FeatureIdNameMapping) = {
    if (symPairs.size != 0) {
      val heading: Seq[String] = "id" +: symPairs.head._1.toSeq.map(_._1).map(fIdNameM.lookupFeatureName(_))

      val tupleArrays = symPairs.flatMap(symPair => {
        val featureValue1: Seq[String] = symPair._1.id.toString +: symPair._1.toSeq.map(_._2)
        val featureValue2: Seq[String] = symPair._2.id.toString +: symPair._2.toSeq.map(_._2)
        Seq(featureValue1, featureValue2, featureValue2.map(x => "-"))
      })
      heading +: tupleArrays
    } else {
      Seq[Seq[String]]()
    }
  }

  def createTupleTable(tuples: Seq[Tuple])(implicit fIdNameM: FeatureIdNameMapping) = {
    val heading: Seq[String] = "id" +: tuples.head.toSeq.map(_._1).map(fIdNameM.lookupFeatureName(_))
    val tupleArrays = tuples.map(tuple => tuple.id.toString +: tuple.toSeq.map(_._2))
    heading +: tupleArrays
  }

  def createSymPairSimVectorTable(pair: Seq[(SymPair[Tuple], Array[Double])])(implicit fIdNameM: FeatureIdNameMapping, featureMeasures: Array[(Int, StringMetric[Double])]) = {
    val featureIdSequence = pair.head._1._1.toSeq.map(_._1)
    val heading: Seq[String] = "id" +: featureIdSequence.map(fIdNameM.lookupFeatureName(_))

    val tupleArrays = pair.flatMap(pair => {
      val featureValue1: Seq[String] = pair._1._1.id.toString +: pair._1._1.toSeq.map(_._2)
      val featureValue2: Seq[String] = pair._1._2.id.toString +: pair._1._2.toSeq.map(_._2)
      val simVector = pair._2
      // Array[(featureId, simValue)]
      val simMetrics: Map[Int, Double] = featureMeasures.map(_._1).zip(simVector).toMap
      val simVectorString: Seq[String] = "-" +: featureIdSequence.map(featureId => {
        val simOption = simMetrics.get(featureId)
        if (simOption.isDefined) {
          val similarity = simOption.get
          f"$similarity%.5f"
        } else {
          "-"
        }
      })
      Seq(featureValue1, featureValue2, simVectorString, simVectorString.map(x => " "))
    })

    heading +: tupleArrays
  }
}