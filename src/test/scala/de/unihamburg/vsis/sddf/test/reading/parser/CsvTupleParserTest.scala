package de.unihamburg.vsis.sddf.test.reading.parser

import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.corpus.PipeReaderTupleCsv

class CsvTupleParserTest extends FunSuite {

  test("CsvTupleParserTest") {
    val line = "\"blub\",\"test   \",\"123\",\",das ist,\""
    val parser = new PipeReaderTupleCsv(Seq(0, 1, FeatureIdNameMapping.Id, 2))
    val tuple = parser.parse(line)
    assert(tuple.id === 123)
    assert(tuple.readFeature(0).get === "blub")
    assert(tuple.readFeature(1).get === "test   ")
    assert(tuple.readFeature(2).get === ",das ist,")
  }

}
