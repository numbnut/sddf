package de.unihamburg.vsis.sddf.test.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

trait LocalSparkContext extends BeforeAndAfterAll { self: Suite =>

  @transient var sc: SparkContext = _

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SddF Test")
      .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

}
