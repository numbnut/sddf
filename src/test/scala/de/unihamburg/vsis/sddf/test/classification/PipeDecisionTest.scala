package de.unihamburg.vsis.sddf.test.classification

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.SddfContext.Duplicate
import de.unihamburg.vsis.sddf.SddfContext.NoDuplicate
import de.unihamburg.vsis.sddf.SddfContext.SymPairSim
import de.unihamburg.vsis.sddf.classification.PipeClassificationDecisionTree
import de.unihamburg.vsis.sddf.classification.PipeClassificationNaiveBayes
import de.unihamburg.vsis.sddf.classification.PipeClassificationSvm
import de.unihamburg.vsis.sddf.pipe.context.SddfPipeContext
import de.unihamburg.vsis.sddf.reading.SymPair
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext

class PipeClassificationTest extends FunSuite with LocalSparkContext with BeforeAndAfterAll{
  
  var input: (SymPairSim, RDD[LabeledPoint]) = _
  
  override def beforeAll() {
    super.beforeAll()
    val tuple1 = Tuple("test1","test1","test1")
    tuple1.id = 1
    val tuple2 = Tuple("test2","test2","test2")
    tuple2.id = 2
    val tuple3 = Tuple("hans","franz","wurst")
    tuple3.id = 3
    
    val symPairSim: SymPairSim = sc.parallelize(Seq(
      (new SymPair(tuple1, tuple2), Array(1D,1D,0D))
      ,(new SymPair(tuple2, tuple3), Array(0D,0D,1D))
    ))
    
    val trainingData: RDD[LabeledPoint] = sc.parallelize(Seq(
      LabeledPoint(label = Duplicate, features = Vectors.dense(Array(0.99,1.0,0.0)))
      ,LabeledPoint(label = Duplicate, features = Vectors.dense(Array(1.0,1.0,0.0)))
      ,LabeledPoint(label = Duplicate, features = Vectors.dense(Array(1.0,0.875,0.0)))
      ,LabeledPoint(label = Duplicate, features = Vectors.dense(Array(1.0,1.0,0.1)))
      ,LabeledPoint(label = Duplicate, features = Vectors.dense(Array(1.0,0.89,0.0)))
      
      ,LabeledPoint(label = NoDuplicate, features = Vectors.dense(Array(0.1,0.0,1.0)))
      ,LabeledPoint(label = NoDuplicate, features = Vectors.dense(Array(0.0,0.2,1.0)))
      ,LabeledPoint(label = NoDuplicate, features = Vectors.dense(Array(0.06,0.0,0.89)))
      ,LabeledPoint(label = NoDuplicate, features = Vectors.dense(Array(0.21,0.19,0.91)))
    ))
    
    input = (symPairSim, trainingData)
  }

  override def afterAll() {
    super.afterAll()
  }
              
	test("naive bayes classification test") {
    val classificationPipe = new PipeClassificationNaiveBayes()
    implicit val pipeContext = new SddfPipeContext()
    val result = classificationPipe.run(input)
    assert(result.count === 1)
  }
  
  test("svm classification test") {
    val classificationPipe = new PipeClassificationSvm()
    implicit val pipeContext = new SddfPipeContext()
    val result = classificationPipe.run(input)
    assert(result.count === 1)
  }

  test("decision tree classification test") {
    val classificationPipe = new PipeClassificationDecisionTree()
    implicit val pipeContext = new SddfPipeContext()
    val result = classificationPipe.run(input)
    assert(result.count === 1)
  }

}