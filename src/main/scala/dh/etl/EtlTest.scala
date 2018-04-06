package dh.etl

object EtlTest extends  App{
  val cfg = new ProcessConfig("D:\\temp\\etl-demo.xml")
  val ctx = new ProcessContext(cfg)
  ctx.process()
}
