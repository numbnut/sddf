package de.unihamburg.vsis.sddf.test.classification

import org.apache.spark.SparkException
import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.sparkextensions.RddUtils
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext

class PipeTrainingDataGeneratorTest extends FunSuite with LocalSparkContext {
  
  test("securly zip test") {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Seq(1, 2, 3))
    
    intercept[SparkException] {
      val result = rdd1.zip(rdd2)
      result.count()
    }
    
    val rdd1RepInvalid = rdd1.repartition(2)
    val rdd2RepInvalid = rdd2.repartition(1)
    
    intercept[IllegalArgumentException] {
    	val result = rdd1RepInvalid.zip(rdd2RepInvalid)
    	result.count()
    }
    
    val result = RddUtils.securlyZipRdds(rdd1, rdd2)
    assert(result.count() === 3)
    
  }

}