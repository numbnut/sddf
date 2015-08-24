package de.unihamburg.vsis.sddf.test.util

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import de.unihamburg.vsis.sddf.pipe.context.SddfPipeContext
import de.unihamburg.vsis.sddf.visualisation.ModelRouterSilent

trait TestSddfPipeContext extends BeforeAndAfterAll { self: Suite =>
  
  @transient implicit var pc: SddfPipeContext = _
  
  override def beforeAll() {
    super.beforeAll()
    pc = new SddfPipeContext(modelRouter = ModelRouterSilent)
  }
  
  override def afterAll() {
    // do nothing
    super.afterAll()
  }
  
}
