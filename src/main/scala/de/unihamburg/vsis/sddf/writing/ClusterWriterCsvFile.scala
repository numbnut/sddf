package de.unihamburg.vsis.sddf.writing

import java.io.File
import java.io.FileWriter

import org.apache.spark.rdd.RDD

import com.opencsv.CSVWriter

import de.unihamburg.vsis.sddf.reading.Tuple

class ClusterWriterCsvFile(file: File, separator: Char = ',') {

  // create folders
  file.getParentFile().mkdirs()

  def this(path: String) = {
    this(new File(path))
  }

  def this(folder: String, file: String) = {
    this(new File(folder, file))
  }

  def write(clusterRdd: RDD[Set[Tuple]]): Unit = {
    val collectedClusters = clusterRdd.collect()
    val writer = new CSVWriter(new FileWriter(file), separator);
    // feed in your array (or convert your data to an array)
    collectedClusters.foreach(set => {
      val tupleIdSet: Set[String] = set.map(tuple => tuple.id.toString())
      val tupleIdArray: Array[String] = tupleIdSet.toArray
      writer.writeNext(tupleIdArray)
    })
    writer.close()
  }
  
}
