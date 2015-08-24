package de.unihamburg.vsis.sddf.sparkextensions

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

object RddUtils {

  /**
   * zip is only allowed on RDDs with equally sized partitions
   * TODO the repartitioning does not scale and is a bottleneck for huge training set sizes
   */
  def securlyZipRdds[A, B: ClassTag](rdd1: RDD[A], rdd2: RDD[B]): RDD[(A, B)] = {
    val rdd1Repartitioned = rdd1.repartition(1)
    val rdd2Repartitioned = rdd2.repartition(1)
    val (rdd1Balanced, rdd2Balanced) = balanceRddSizes(rdd1Repartitioned, rdd2Repartitioned)
    rdd1Balanced.zip(rdd2Balanced)
  }

  def balanceRddSizes[A, B](rdd1: RDD[A], rdd2: RDD[B]): (RDD[A], RDD[B]) = {
    val rdd1count = rdd1.count()
    val rdd2count = rdd2.count()
    val difference = math.abs(rdd1count - rdd2count).toInt
    if (rdd1count > rdd2count) {
      (removeRandomElements(rdd1, difference), rdd2)
    } else if (rdd2count > rdd1count) {
      (rdd1, removeRandomElements(rdd2, difference))
    } else {
      (rdd1, rdd2)
    }
  }

  def removeRandomElements[A](rdd: RDD[A], numberOfElements: Int): RDD[A] = {
    val sample: Array[A] = rdd.takeSample(false, numberOfElements)
    val set: Set[A] = Set(sample: _*)
    rdd.filter(x => if (set.contains(x)) false else true)
  }

}