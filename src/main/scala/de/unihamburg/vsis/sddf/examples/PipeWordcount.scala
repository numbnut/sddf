package de.unihamburg.vsis.sddf.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import com.rockymadden.stringmetric.StringMetric
import de.unihamburg.vsis.sddf.pipe.PipeElement
import de.unihamburg.vsis.sddf.pipe.context.AbstractPipeContext
import de.unihamburg.vsis.sddf.visualisation.model.BasicAnalysable

class PipeWordcount()
  extends PipeElement[RDD[String], RDD[(String, Int)]] {

  def step(input: RDD[String])(implicit pipeContext: AbstractPipeContext): RDD[(String, Int)] = {
    // flatten the collection of word arrays
    val words = input.flatMap(line => line.split(" "))
    // initialize the counter of each word with one
    val wordsWithCounter = words.map(word => (word, 1))
    // add up all counters of the same word
    wordsWithCounter.reduceByKey(_ + _)
  }

}

// companion object for a better usability
object PipeWordcount {
  def apply() = new PipeWordcount()
}