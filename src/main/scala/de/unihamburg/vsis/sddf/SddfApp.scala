package de.unihamburg.vsis.sddf

import org.joda.time.format.PeriodFormatterBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import de.unihamburg.vsis.sddf.config.Config

import scopt.Read
import scopt.OptionParser

class SddfApp extends App {

  val periodFormatter = (new PeriodFormatterBuilder() minimumPrintedDigits (2) printZeroAlways ()
    appendDays () appendSeparator ("d ")
    appendHours () appendSeparator (":") appendMinutes () appendSuffix (":") appendSeconds ()
    appendSeparator (".")
    minimumPrintedDigits (3) appendMillis () toFormatter)

  @transient var _log: Logger = null
  // Method to get or create the logger for this object
  def log(): Logger = {
    if (_log == null) {
      _log = LoggerFactory.getLogger(getClass.getName)
    }
    _log
  }
  
  @transient var _logLineage: Logger = null
  // Method to get or create the logger for this object
  def logLineage(): Logger = {
    if (_logLineage == null) {
      _logLineage = LoggerFactory.getLogger("lineage")
    }
    _logLineage
  }
  

  // extend Parser to accept the type Option
  implicit val optionRead: Read[Option[String]] = Read.reads(Some(_))
  
  // parsing commandline parameters
  val parser = new OptionParser[Parameters]("sddf") {
    head("SddF", "0.1.0")
    opt[Map[String, String]]('p', "properties") optional() valueName("<property>") action { (x, c) =>
      c.copy(properties = x) } text("set arbitrary properties via command line")
    opt[Option[String]]('c', "config-file") optional() action { (x, c) =>
      c.copy(propertyPath = x) } text("optional path to a property file")
  }
  
  // parser.parse returns Option[C]
  val parameters = parser.parse(args, Parameters())
  var propertiesCommandline: Map[String, String] = Map()
  var propertiesPath: Option[String] = None
   parameters match {
    case Some(config) =>
      propertiesCommandline = config.properties
      propertiesPath = config.propertyPath
    case None =>
      // arguments are bad, error message will have been displayed
  }
  
  val Conf: Config = if(propertiesPath.isDefined) new Config(propertiesPath.get) else new Config()
  
  propertiesCommandline.foreach(props => {
	  Conf.setPropertyCommandline(props._1, props._2)
  })
  
}

case class Parameters(propertyPath: Option[String] = None, properties: Map[String,String] = Map())

