package de.unihamburg.vsis.sddf.test.util

import org.apache.spark.rdd.RDD
import org.scalatest.Suite

import de.unihamburg.vsis.sddf.SddfContext.pairToInt
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorRemoveRegex
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorTrim
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Id
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Ignore
import de.unihamburg.vsis.sddf.reading.corpus.PipeReaderTupleCsv

trait MusicbrainzSchema extends TestSddfPipeContext { self: Suite =>

  val Number = (0, "number")
  val Title = (1, "title")
  val Length = (2, "length")
  val Artist = (3, "artist")
  val Album = (4, "album")
  val Year = (5, "year")
  val Language = (6, "language")

  val featureIdNameMapping = Map(Number, Title, Length, Artist, Album, Year, Language)

  implicit val featureIdNameMapper = new FeatureIdNameMapping(featureIdNameMapping)

  def parseTuples(input: RDD[String]) = {
    // Parse Tuples
    val allFields: Seq[Int] = Seq(Number, Title, Length, Artist, Album, Year, Language)
    val allFieldsWithId: Seq[Int] = Ignore +: Id +: Ignore +: allFields

    val pipe = PipeReaderTupleCsv(allFieldsWithId)
      .append(PipePreprocessorTrim(allFields: _*))
      .append(PipePreprocessorRemoveRegex("[^0-9]", Number, Year, Length))

    pipe.run(input)

  }

}
