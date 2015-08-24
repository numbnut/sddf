package de.unihamburg.vsis.sddf.reading

/**
 * Basic IdConverter implementation that only parses a Long out of the id String.
 */
object IdConverterBasic extends IdConverter with Serializable {
  
  def convert(id: String): Long = {
    Long2long(java.lang.Long.parseLong(id))
  }

  def convert(id: Long): String = {
    id.toString()
  }
  
  override val name = "IdConverterBasic"
  
}
