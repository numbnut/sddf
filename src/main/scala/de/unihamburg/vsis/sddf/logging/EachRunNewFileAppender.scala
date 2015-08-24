package de.unihamburg.vsis.sddf.logging

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.FileAppender

import de.unihamburg.vsis.sddf.config.Config

class EachRunNewFileAppender extends FileAppender {
  override def setFile(fileName: String, append: Boolean, bufferedIO: Boolean, bufferSize: Int) = {
    val oldFile = new File(fileName)
    val dir = if (oldFile.isDirectory()) oldFile else oldFile.getParentFile
    val fileSuffix = if (oldFile.isDirectory()) ".log" else oldFile.getName
    val newFileName = EachRunNewFileAppender.runUuid + fileSuffix
    val newLogFile = new File(dir, newFileName)
    super.setFile(newLogFile.getPath, append, bufferedIO, bufferSize)
  }
}

object EachRunNewFileAppender {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
  val runUuid = System.currentTimeMillis().toString() + "-" + dateFormat.format(new Date())
}