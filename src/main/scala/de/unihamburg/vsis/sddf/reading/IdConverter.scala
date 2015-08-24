package de.unihamburg.vsis.sddf.reading

import de.unihamburg.vsis.sddf.Parameterized

trait IdConverter extends Parameterized {

  def convert(id: String): Long

  def convert(id: Long): String
  
  override val paramMap: Map[String, Any] = Map()

}
