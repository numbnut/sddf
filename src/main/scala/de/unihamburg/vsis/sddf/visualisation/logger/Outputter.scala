package de.unihamburg.vsis.sddf.visualisation.logger

import org.joda.time.DateTime
import org.joda.time.Period
import org.joda.time.format.PeriodFormatterBuilder

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.visualisation.model.Analysable
import de.unihamburg.vsis.sddf.Parameterized

abstract class Outputter extends Logging {

  // TODO: get decimal separator for the period formatter
  //  private val sep = DecimalFormat.getInstance().getDecimalFormatSymbols().getDecimalSeparator();

  val periodFormatter = (new PeriodFormatterBuilder() minimumPrintedDigits (2) printZeroAlways ()
    appendDays () appendSeparator ("d ")
    appendHours () appendSeparator (":") appendMinutes () appendSuffix (":") appendSeconds ()
    appendSeparator (".")
    minimumPrintedDigits (3) appendMillis () toFormatter)

  // Adjust this to stretch the log output
  protected val lineWidth = 80

  protected val halfLineWidth = lineWidth / 2

  protected val keyLength = halfLineWidth - 2

  protected val valueLength = keyLength

  def doubleFormatter(value: Double): String = {
    "%" + valueLength + ".9f" format value
  }

  def longFormatter(value: Long): String = {
    "%" + valueLength + "d" format value
  }

  def stringFormatter(value: String): String = {
    "%" + valueLength + "s" format value
  }

  def printLogLine(key: String, value: String) = {
    log.info("# " + ("%-" + keyLength + "s" format (key + ": ")) + ("%" + valueLength + "s" format value) + " #")
  }
  
  def printMap(map: Map[_, _]): Unit = {
    map.foreach(pair => {
      val name = pair._1
      val value = pair._2
      value match {
        case v: Parameterized => {
          printLogLine(pair._1.toString, "")
          printMap(v.paramMap)
        }
        case _ => printLogLine(pair._1.toString, pair._2.toString)
      }
    })
  }

  def printHeading(analysable: Analysable) = {
    var heading = "##### " + analysable.name + " "
    for (i <- heading.length() until lineWidth) heading += '#'
    log.info(heading)
  }

  def printFooter() = {
    var footer = ""
    for (i <- 1 to lineWidth) footer += '#'
    log.info(footer)
  }

  def logCustomResults(analysable: Analysable): Unit
  
  def logResults(analysable: Analysable) = {
    printHeading(analysable)
    logCustomResults(analysable)
    printLightSeparator()
    val analyseDuration = new Period(analysable.startTime, new DateTime)

    printLogLine("Runtime", stringFormatter(periodFormatter.print(analyseDuration)))
    printFooter()
    log.info(arrowLine)
  }
  
  private val arrowLine: String = {
    val arrows = "V"
    val arrowPaddingLeft = (lineWidth - arrows.length) / 2
    val arrowPaddingRight = lineWidth - arrows.length - arrowPaddingLeft
    var arrowLine = ""
    for (i <- 1 to arrowPaddingLeft) arrowLine += ' '
    arrowLine += arrows
    for (i <- 1 to arrowPaddingRight) arrowLine += ' '
    arrowLine
  }

  protected def printLightSeparator() = {
    var sep = "#"
    for (i <- 2 to lineWidth - 1) sep += '-'
    sep += "#"
    log.info(sep)
  }
  
}
