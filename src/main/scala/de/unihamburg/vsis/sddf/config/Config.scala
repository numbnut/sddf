package de.unihamburg.vsis.sddf.config

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConversions._

import org.apache.commons.configuration.CompositeConfiguration
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class Config(customPropertiesPath: String = "project.properties") {

  private val Config = new CompositeConfiguration()

  private val ConfigFileNameDefault = "project-default.properties"

  /**
   * Properties set by a setter during runtime have the highest priority.
   */
  val innerApplication = new PropertiesConfiguration()

  /**
   * Commandline params have the second highest priority.
   */
  val customCommandline = new PropertiesConfiguration()

  /**
   * Properties in the custom file right behind the commandline params.
   */
  private val customFile = new PropertiesConfiguration(customPropertiesPath)

  /**
   * The properties from the default file are the last ones.
   */
  private val defaultFile = new PropertiesConfiguration(ConfigFileNameDefault)

  Config.addConfiguration(innerApplication)
  Config.addConfiguration(customCommandline)
  Config.addConfiguration(customFile)
  Config.addConfiguration(defaultFile)

  private val _conf = new SparkConf().setAppName("SddF")
    .setMaster(Config.getString("spark.conf.master", "local"))
  // enable kryo serializer
  //  _conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //  _conf.registerKryoClasses(Array(
  //    classOf[de.unihamburg.vsis.sddf.reading.TupleArray]
  //    , classOf[Array[String]]
  //    , classOf[de.unihamburg.vsis.sddf.reading.Tuple]
  //    , classOf[de.unihamburg.vsis.sddf.reading.SymPair[Any]]
  //    , classOf[Array[scala.Tuple3[Any, Any, Any]]]
  //    , classOf[Array[Int]]
  //    , classOf[scala.reflect.ClassTag$$anon$1]
  //    , classOf[java.lang.Class[_]]
  //    , classOf[Array[scala.Tuple2[Any, Any]]]
  //    , classOf[None$]
  //    , classOf[Array[Double]]
  //      ))
  //  GraphXUtils.registerKryoClasses(_conf)
  //  _conf.set("spark.kryo.registrationRequired", "true")

  // Functions
  /**
   * Returns a new SparkContext
   */
  def newSparkContext(): SparkContext = {
    new SparkContext(_conf)
  }

  /**
   * Generates at startup a pseudo unique identifier for this run.
   */
  val runUuid = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    System.currentTimeMillis().toString() + "-" + dateFormat.format(new Date())
  }

  // Getter
  def inputFile(name: String) = Config.subset("input.source").getString(name)

  def inputFiles(name: String) = Config.subset("input.source").getList(name)
    .asInstanceOf[java.util.List[String]].toList

  def goldstandardFile(name: String) = Config.subset("input.goldstandard").getString(name)

  def resultDirectory = Config.getString("result.directory")

  def writeResultClusterToDisk = Config.getBoolean("sddf.result.write")

  def analyse = Config.getBoolean("sddf.analyse")

  def desiredWorkerCount() = Config.getInt("sddf.desired.worker.count")

  def clusterSetupDescrition() = Config.getString("sddf.cluster.setup.description")

  // Setter
  /**
   * generic setter to be called directly or by a setter of a certain property.
   */
  def setProperty(key: String, value: Any): Unit = {
    innerApplication.setProperty(key, value)
  }

  def setPropertyCommandline(key: String, value: Any): Unit = {
    customCommandline.setProperty(key, value)
  }

}
