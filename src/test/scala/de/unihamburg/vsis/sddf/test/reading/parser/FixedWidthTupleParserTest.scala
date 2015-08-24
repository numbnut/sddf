package de.unihamburg.vsis.sddf.test.reading.parser

import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.IdConverterBasic
import de.unihamburg.vsis.sddf.reading.corpus.PipeReaderTupleFixedWidth

class FixedWidthTupleParserTest extends FunSuite {

  test("FixedWidthTupleParserWithTrim") {
    val line = "blam      blab123    tunk"
    val parser = new PipeReaderTupleFixedWidth(
      Seq(0, 1, FeatureIdNameMapping.Id, 2),
      IdConverterBasic,
      Seq(4, 10, 3, 8),
      trim = true
    )
    val tuple = parser.parse(line)
    assert(tuple.id === 123)
    assert(tuple.readFeature(0).get === "blam")
    assert(tuple.readFeature(1).get === "blab")
    assert(tuple.readFeature(2).get === "tunk")
  }

  test("FixedWidthTupleParserWithoutTrim") {
    val line = "blam      blab123    tunk"
    val parser = new PipeReaderTupleFixedWidth(
      Seq(0, 1, FeatureIdNameMapping.Id, 2),
      IdConverterBasic,
      Seq(4, 10, 3, 8),
      trim = false
    )
    val tuple = parser.parse(line)
    assert(tuple.id === 123)
    assert(tuple.readFeature(0).get === "blam")
    assert(tuple.readFeature(1).get === "      blab")
    assert(tuple.readFeature(2).get === "    tunk")
  }

}
