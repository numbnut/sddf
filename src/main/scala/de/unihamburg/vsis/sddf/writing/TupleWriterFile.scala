package de.unihamburg.vsis.sddf.writing

import java.io.File
import java.io.FileWriter

import org.apache.spark.rdd.RDD

import com.opencsv.CSVWriter

import de.unihamburg.vsis.sddf.reading.Tuple

/**
 * Created by niklas on 26.08.14.
 */
class TupleWriterFile(file: File, separator: Char = ',') {

  val writer = new CSVWriter(new FileWriter(file), separator);

  def writeTuple[A <: Tuple](tuple: A): Unit = {
    writer.writeNext(tuple.id.toString +: tuple.toSeq.map(_._2).toArray)
  }

  def close() = {
	  writer.close()
  }
  
  def blankLine() = {
    writer.writeNext(Array())
  }
  
  def writeTuple[A <: Tuple](tuples: Traversable[A]): Unit = {
    tuples.foreach(tuple => {
      writer.writeNext(tuple.id.toString +: tuple.toSeq.map(_._2).toArray)
    })
  }

  def writeTuple[A <: Tuple](tuples: RDD[A]): Unit = {
    val collectedTuples = tuples.collect()
    collectedTuples.foreach(tuple => {
      writer.writeNext(tuple.id.toString +: tuple.toSeq.map(_._2).toArray)
    })
  }
}
