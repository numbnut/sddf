package de.unihamburg.vsis.sddf.reading.corpus

import au.com.bytecode.opencsv.CSVParser

import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.reading.IdConverter
import de.unihamburg.vsis.sddf.reading.IdConverterBasic

/**
 * Simple csv Tuple which uses opencsv to parse the lines.
 */
class PipeReaderTupleCsv(
    featureIds: Seq[Int],
    idConverter: IdConverter = IdConverterBasic,
    seperator: Char = ',',
    quotechar: Char = '"')
  extends AbstractPipeReaderTuple(featureIds, idConverter)
  with Parameterized {

  _analysable.algo = this
  override val paramMap = Map("featureIds" -> featureIds,
    "idConverter" -> idConverter.getParameterString, "seperator" -> seperator,
    "quotechar" -> quotechar)

  /*
   * Needs to be transient, because the CSVParser can't be serialized.
   */
  @transient private var _parser: CSVParser = null

  // Method to get or create the parser for this object
  protected def parser: CSVParser = {
    if (_parser == null) {
      _parser = new CSVParser(seperator, quotechar)
    }
    _parser
  }

  def extractValues(line: String): Seq[String] = {
    parser.parseLine(line)
  }

}

object PipeReaderTupleCsv {
  def apply(
    featureIds: Seq[Int],
    idConverter: IdConverter = IdConverterBasic,
    seperator: Char = ',',
    quotechar: Char = '"') = {
    new PipeReaderTupleCsv(featureIds, idConverter, seperator, quotechar)
  }
}
