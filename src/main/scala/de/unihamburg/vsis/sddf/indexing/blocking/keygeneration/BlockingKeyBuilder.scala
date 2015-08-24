package de.unihamburg.vsis.sddf.indexing.blocking.keygeneration

import de.unihamburg.vsis.sddf.Parameterized
import de.unihamburg.vsis.sddf.reading.Tuple

trait BlockingKeyBuilder extends Parameterized {

  def buildBlockingKey(tuple: Tuple): String

}
