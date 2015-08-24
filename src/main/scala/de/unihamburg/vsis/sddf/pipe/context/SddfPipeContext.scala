package de.unihamburg.vsis.sddf.pipe.context

import org.apache.spark.rdd.RDD
import org.joda.time.Period

import de.unihamburg.vsis.sddf.visualisation.ModelRouter
import de.unihamburg.vsis.sddf.visualisation.logger.ModelRouterLogging

class SddfPipeContext(
    val name: String = "Unnamed Pipeline",
    modelRouter: ModelRouter = ModelRouterLogging)
  extends AbstractPipeContext(modelRouter)
  with CorpusContext
  with GoldstandardContext
  with ResultContext {
  
  var runtime: Option[Period] = None
  var filepath: Option[String] = None
      
  val persistedRDDs = new scala.collection.mutable.HashMap[String, RDD[_]]()
  
}