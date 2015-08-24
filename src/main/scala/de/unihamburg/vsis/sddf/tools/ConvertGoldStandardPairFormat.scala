package de.unihamburg.vsis.sddf.tools

import java.io.File
import java.io.PrintWriter

import scala.io.Source

/**
 * @TODO check why there are only tuples of the size 2
 */
object ConvertGoldStandardPairFormat {
  def main(args: Array[String]) {
    val corpusInputPath = "/home/niklas/projects/demood/data/list_ABC_small.txt"

    val gsInputPath = "/home/niklas/projects/demood/data/Duplicate_Pairs_small.txt"
    val gsOutputPath = "/home/niklas/projects/demood/data/Duplicate_Pairs_small_tuples_expanded.txt"

    val corpusMap = createCorpus(corpusInputPath)
    val duplicateMap = createDuplicateMap(gsInputPath, corpusMap)
    writeMapAndRemoveElements(duplicateMap, gsOutputPath)
  }

  def createCorpus(corpusInputPath: String) = {
    // create CORPUS
    // maps from id to the whole tuple line
    val corpusMap = scala.collection.mutable.Map[String, String]()
    Source.fromFile(corpusInputPath).getLines().foreach(line => {
      var id = ""
      if (line.startsWith("\"")) {
        id = line.drop(1).substring(0, 7)
      } else {
        id = line.substring(0, 7)
      }
      corpusMap += id -> line
    })
    corpusMap
  }

  def createDuplicateMap(
    gsInputPath: String,
    corpusMap: scala.collection.mutable.Map[String, String]
  ) = {

    // create duplicate map
    val duplMap = scala.collection.mutable.Map[String, scala.collection.mutable.HashSet[String]]()
    Source.fromFile(gsInputPath).getLines().foreach(line => {
      val parts = line.split(",")
      val id1 = parts(0) trim
      val id2 = parts(1) trim
      val line1 = corpusMap.get(id1).get
      val line2 = corpusMap.get(id2).get
      var set1option = duplMap.get(id1)
      var set2option = duplMap.get(id2)
      var set: scala.collection.mutable.HashSet[String] = null
      // both ids are new and sets are not present
      if (!set1option.isDefined && !set2option.isDefined) {
        // create new set
        set = scala.collection.mutable.HashSet[String]()
        duplMap += id1 -> set
        duplMap += id2 -> set
        //        duplicateMap += id1 -> set
        //        duplicateMap += id2 -> set
        // both ids and sets are present
      } else if (set1option.isDefined && set2option.isDefined) {
        // merge the two sets
        set = scala.collection.mutable.HashSet[String]()
        var set1 = set1option.get
        var set2 = set2option.get
        set1 ++= set2
        set2 ++= set1
        //        duplicateMap += id1 -> set
        //        duplicateMap += id2 -> set
      } else if (!set1option.isDefined && set2option.isDefined) {
        set = set2option.get
        duplMap += id1 -> set
        //        duplicateMap += id1 -> set
      } else {
        set = set1option.get
        duplMap += id2 -> set
        //        duplicateMap += id2 -> set
      }
      set += line1
      set += line2
    })
    duplMap
  }

  def writeMapAndRemoveElements(
    duplicateMap: scala.collection.mutable.Map[String, scala.collection.mutable.HashSet[String]],
    gsOutputPath: String
  ) = {
    // write out duplicate map
    val writer = new PrintWriter(new File(gsOutputPath))
    duplicateMap.foreach(mapping => {
      var nothingWritten = true
      mapping._2.foreach(line => {
        writer write line + "\n"
        nothingWritten = false
      })
      if (!nothingWritten) {
        writer write "\n"
      }
      // empty set to avoid outputting it multiple times
      mapping._2.clear
    })

    writer.close()
  }
}

