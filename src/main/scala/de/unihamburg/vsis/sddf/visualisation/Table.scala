package de.unihamburg.vsis.sddf.visualisation

object Table {

  def printTable(table: Seq[Seq[Any]]): Unit = {
    val sizes: Seq[Seq[Int]] = for (row <- table) yield for (cell <- row) yield if (cell == null) 1 else cell.toString.length()
    val maxColSizes = for (col <- sizes.transpose) yield col.max
    for (row <- table) {
      row.zip(maxColSizes).foreach(pair => {
        val value = if (pair._1 == null) {
          "-"
        } else {
          pair._1
        }
        val formatString = "|%" + pair._2 + "s"
        printf(formatString, value)
      })
      print("|\n")
    }
  }
  
}