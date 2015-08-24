package de.unihamburg.vsis.sddf.tools

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.unihamburg.vsis.sddf.logging.Logging
import de.unihamburg.vsis.sddf.pipe.context.SddfPipeContext
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Id
import de.unihamburg.vsis.sddf.reading.corpus.PipeStoreInContextCorpus
import de.unihamburg.vsis.sddf.reading.corpus.PipePrintSampleCorpus
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.corpus.PipeReaderTupleCsv
import de.unihamburg.vsis.sddf.writing.TupleWriterFile

/**
*	Does not work at the moment.
*	I used gnu sort to achieve uniquenes.
*/
object ExactDuplicateFilter extends App with Logging {

  if (args.size == 1 && (new File(args(0))).exists()) {
    val conf = new SparkConf().setAppName("ExactDuplicateFilter")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    implicit val pipeContext = new SddfPipeContext
      
    val Content: (Int, String) = (0, "content")

    val featureMapping: Map[Int, String] = Map(Content)

    implicit val featureIdNameMapper = new FeatureIdNameMapping(featureMapping)

    val inputFileKey = "musicbrainz"

    // Parse Tuples
    val allFields: Seq[Int] = Seq(Content._1)
    val allFieldsWithId: Seq[Int] = Id +: allFields

    val parserPipe = new PipeTupleParserCsvIdContent(allFieldsWithId)
    val pipe = parserPipe.append(PipeStoreInContextCorpus()).append(PipePrintSampleCorpus())
    pipe.start(sc.textFile(args(0)))
    val result: RDD[Tuple] = parserPipe.output.get
    val resultCount = result.count
    log.info("Lines parsed: " + resultCount)
    
    val distinct = result.distinct()
    val distinctCount = distinct.count
    log.info("Distinct Lines Count: " + distinctCount)
    log.info("Lines removed: " + (resultCount - distinctCount))
    
    val tupleWriter = new TupleWriterFile(new File(args(0) + ".distinct"))
    tupleWriter.writeTuple(distinct)

  } else {
    println("Please provide a valid file path.")
  }

}

class PipeTupleParserCsvIdContent(featureIds: Seq[Int]) extends PipeReaderTupleCsv(featureIds) {
  override def extractValues(line: String): Seq[String] = {
    val splitted = parser.parseLine(line)
    Seq(splitted.head, splitted.tail.mkString(","))
  }
}

