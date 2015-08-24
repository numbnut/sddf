package de.unihamburg.vsis.sddf.reading.corpus

import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.reading.IdConverter
import de.unihamburg.vsis.sddf.reading.IdConverterBasic

class PipeReaderTupleFixedWidth(
    featureIds: Seq[Int],
    idConverter: IdConverter = IdConverterBasic,
    widths: Seq[Int],
    trim: Boolean)
  extends AbstractPipeReaderTuple(featureIds, idConverter)
  with Parameterized {

  _analysable.algo = this
  
  override val paramMap = Map("featureIds" -> featureIds,
      "idConverter" -> idConverter, "widths" -> widths, "trim" -> trim)

  def extractValues(line: String): Seq[String] = {
    var start = 0;
    widths.map(width => {
      val result = line.substring(start, start + width)
      start += width
      if (trim) {
        result.trim()
      } else {
        result
      }
    })
  }

}

object PipeReaderTupleFixedWidth {
  
  def apply(
      featureIds: Seq[Int],
      idConverter: IdConverter = IdConverterBasic,
      widths: Seq[Int],
      trim: Boolean) = {
    new PipeReaderTupleFixedWidth(featureIds, idConverter, widths, trim)
  }

}