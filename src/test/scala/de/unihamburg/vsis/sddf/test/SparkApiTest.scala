package de.unihamburg.vsis.sddf.test

import org.apache.spark.rdd.RDD
import org.scalatest.Finders
import org.scalatest.FunSuite
import de.unihamburg.vsis.sddf.SddfContext.pairToInt
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorRemoveRegex
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorTrim
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Id
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Ignore
import de.unihamburg.vsis.sddf.reading.corpus.PipeReaderTupleCsv
import de.unihamburg.vsis.sddf.test.util.LocalSparkContext
import de.unihamburg.vsis.sddf.test.util.MusicbrainzSchema

class SparkApiTest extends FunSuite with LocalSparkContext with MusicbrainzSchema {

  test("test rdd substraction") {

    val file1 = sc.textFile("src/test/resources/musicbrainz-10.csv.dup")
    val file2 = sc.textFile("src/test/resources/musicbrainz-10.csv.dup")
    
    val data1 = parseTuples(file1)
    assert(data1.count() === 10)
    val data2 = parseTuples(file2)
    assert(data2.count() === 10)
    val substraction = data1.subtract(data2)
    assert(substraction.count() === 0)
  }

}
