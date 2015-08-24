package de.unihamburg.vsis.sddf.test.blocking.keygeneration

import org.scalatest.FunSuite

import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilderBasic
import de.unihamburg.vsis.sddf.reading.Tuple
import de.unihamburg.vsis.sddf.reading.TupleArray

class BlockingKeyBuilderBasicTest extends FunSuite {

  val AddressCity = (0, "city")
  val AddressHousenumber = (1, "house-nr")
  val AddressRoad = (2, "road")
  val AddressRoadAndNumber = (3, "road+no")
  val AddressStateAcronym = (4, "state-acronym")
  val AddressState = (5, "state")
  val AddressZipCode = (6, "zip")
  val Birthday = (7, "bday")
  val FirstName = (8, "fname")
  val FirstAndMiddleName = (9, "f+mname")
  val FullName = (10, "name")
  val Givenname = (11, "gname")
  val MiddleName = (12, "mname")
  val Postbox1 = (13, "pb1")
  val Postbox2 = (14, "pb2")
  val Phone = (15, "phone")
  val Ssn = (16, "ssn")

  test("testing normal behavior BlockingKeyBuilder") {
    // first four chars
    val nameFirstFourChars = (FullName._1, 0 to 3)
    val birthdayAll = (Birthday._1, 0 to 100)
    val ssnAll = (Ssn._1, 0 to 100)
    val bkvBuilder = new BlockingKeyBuilderBasic(nameFirstFourChars, birthdayAll, ssnAll)

    val tuple1: Tuple = new TupleArray(17)
    tuple1.id = 1
    tuple1.addFeature(FullName._1, "Vorname Nachname")
    tuple1.addFeature(Birthday._1, "21.06.1967")
    tuple1.addFeature(Ssn._1, "987234764")
    val tuple2: Tuple = new TupleArray(17)
    tuple2.id = 2
    tuple2.addFeature(FullName._1, "Heino Pfeiffer")
    tuple2.addFeature(Birthday._1, "13.08.2004")
    tuple2.addFeature(Ssn._1, "134832749")

    val bkv1 = bkvBuilder.buildBlockingKey(tuple1)
    val bkv2 = bkvBuilder.buildBlockingKey(tuple2)

    assert("Vorn21.06.1967987234764" === bkv1)
    assert("Hein13.08.2004134832749" === bkv2)

  }

  test("testing false input") {
    // first four chars
    val nameFirstFourChars = (FullName._1, -1 to -10 by -1)
    val bkvBuilder = new BlockingKeyBuilderBasic(nameFirstFourChars)

    val tuple1: Tuple = new TupleArray(17)
    tuple1.id = 1
    tuple1.addFeature(FullName._1, "Vorname Nachname")

    val bkv1 = bkvBuilder.buildBlockingKey(tuple1)

    assert("" === bkv1)
  }

}
