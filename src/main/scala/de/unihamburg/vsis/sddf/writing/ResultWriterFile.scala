package de.unihamburg.vsis.sddf.writing

import java.io.File
import java.io.FileWriter

import com.opencsv.CSVWriter


/**
 * Created by niklas on 26.08.14.
 */
class ResultWriterFile(file: File, separator: Char = '\t') {

  val writer = new FileWriter(file)
  
  val csvWriter = new CSVWriter(writer, separator);

  def close() = {
    csvWriter.close()
  }
  
  def writeCommentLine(comment: String) = {
    writeLine("#" + comment)
  }
  
  def writeLine(str: String) = {
    csvWriter.flush
    writer.write(str + "\n")
    writer.flush()
  }

  def blankLine() = {
    csvWriter.writeNext(Array())
    csvWriter.flush()
  }

  def writeResult(result: Seq[_], withQuotes: Boolean = false): Unit = {
    csvWriter.writeNext(result.map(_.toString()).toArray, withQuotes)
  }

  def writeResults(result: Seq[Seq[_]], withQuotes: Boolean = false): Unit = {
    result.foreach(writeResult(_, withQuotes))
  }
}
