package dh.etl

import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source

/**
  * Created by Administrator on 2015/5/26.
  */
class ProcessContext(val config: ProcessConfig) extends Logging {
  type RecordMap = mutable.HashMap[String, String]
  val sourceFile: String = config.sourceFile
  val sourceFields: Seq[String] = config.sourceFields
  val sourceDelimiter: String = config.sourceDelimiter
  val targetFields: Seq[String] = config.targetFields
  val targetDelimiter: String = config.targetDelimiter
  val mapper: Map[String, Mapper] = config.getMapper
  val (functions, filters) = config.getFunctions

  //private val sc_ = initSparkContext(cfg.id)

  def initSparkContext(appName: String): SparkContext = {
    val conf = new SparkConf().setMaster("local").setAppName(appName)
    conf.set("spark.cores.max", "32")
    conf.set("spark.eventLog.enabled ", "true")
    val sc = new SparkContext(conf)
    sc
  }

  def process(): Unit = {
    //operate RDD here
    for (line <- Source.fromFile(sourceFile).getLines()) {
      val output = processLine(line)
      output match {
        case None =>
        case Some(value) => value
      }
    }
  }

  def processLine(line: String): Option[String] = {
    logInfo("input:  " + line)
    val fields = line.split(sourceDelimiter)
    if (fields.length != sourceFields.length) {
      logError("Source fields configuration error" + fields.mkString(","))
      throw new ConfigurationException("Source fields configuration error")
    }
    else {
      val data = new RecordMap()
      sourceFields.zip(fields).foreach {
        case (k, v) => data(k) = v
      }

      if (doFilter(filters, data))
        None
      else {
        functions.foreach(_ (this, data))

        val output = targetFields.map { field =>
          data.get(field) match {
            case None =>
              logError("Target fields configuration eror")
              throw new ConfigurationException("target fields")
            case Some(value) => value
          }
        }.mkString(targetDelimiter)

        logInfo("output: " + output)
        Some(output)
      }
    }
  }

  @tailrec final def doFilter(filters: List[FilterDescription], data: RecordMap): Boolean =
    filters match {
      case List() => true
      case x :: tail =>
        if (x(this, data)) false
        else doFilter(tail, data)
    }
}