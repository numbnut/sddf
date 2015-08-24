package de.unihamburg.vsis.sddf.reading

object IdConverterHex extends IdConverter with Serializable {

  def convert(id: String): Long = {
    Long2long(java.lang.Long.parseLong(id, 16))
  }

  def convert(id: Long): String = {
    java.lang.Long.toHexString(id).toUpperCase()
  }
  
  override val name = "Hex Id Converter"

}
