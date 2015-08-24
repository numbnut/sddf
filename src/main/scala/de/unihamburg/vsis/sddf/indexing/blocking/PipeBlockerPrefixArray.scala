package de.unihamburg.vsis.sddf.indexing.blocking

import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilder
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.visualisation.model.AlgoAnalysable
import de.unihamburg.vsis.sddf.Parameterized

/**
 * Creates a SuffixArrayBlocker using the bkvBuilder function to create
 * the BlockingKeyValue (BKV) for each Tuple.
 */
class PipeBlockerPrefixArray(
    minimumPrefixLength: Int,
    maximumBlockSize: Int
    )(implicit bkvBuilder: BlockingKeyBuilder)
  extends PipeBlockerSuffixArray(minimumPrefixLength, maximumBlockSize)
  with Parameterized {

  /**
   * Calculates the suffixes of a given BKV.
   * Longest suffix is the BKV it self.
   * Shortest suffix is the one with the size == minimumSuffixLength.
   */
  override def calcSuffixes(bkvTuplePair: (String, Tuple)): Seq[(String, Tuple)] = {
    super.calcSuffixes((bkvTuplePair._1.reverse, bkvTuplePair._2))
  }
  
  @transient override val _analysable = new AlgoAnalysable
  _analysable.algo = this
  _analysable.name = this.name
  override val name = "PrefixArrayBlocker"
  override val paramMap = Map[String, Any]("minimumPrefixLength" -> minimumPrefixLength,
      "maximumBlockSize" -> maximumBlockSize, "BlockingKeyBuilder" -> bkvBuilder.getParameterString)
  
}
