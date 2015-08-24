package de.unihamburg.vsis.sddf.writing

import java.io.File

class ClusterWriterCsvFileActualDate(folder: String) extends ClusterWriterCsvFile({
  val format = new java.text.SimpleDateFormat("dd-MM-yyyy-HH-mm-ss")
  new File(folder, format.format(new java.util.Date()) + ".csv")
})
