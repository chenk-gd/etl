package dh.etl

import org.apache.spark.Logging

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.language.postfixOps
import scala.xml.NodeSeq

/**
  * Created by Administrator on 2015/5/22.
  */
class ProcessConfig(val configFile: String) extends Logging {
  logInfo("initialize ProcessConfig from " + configFile)
  private val nameAttribute = "name"
  private val outputAttribute = "output"

  private val config_ = xml.XML.loadFile(configFile)
//  private val id = config_ \ "@id" text

  private val dimensionTable = config_ \\ "dimensionTable"

  private val sourceMetaData_ = config_ \\ "sourceMetaData"
  val sourceDelimiter: String = sourceMetaData_ \ "@delimiter" text
  val sourceFile: String = (sourceMetaData_ \\ "inputPath").head \ "@path" text
  val sourceFieldsx: Seq[String] = (sourceMetaData_ \\ "field" \\ "@id").map(_.text)
  val sourceFields: Seq[String] = sourceMetaData_ \ "fields" size match {
    case 0 =>
      val n = fetchOneLine(sourceFile).split(sourceDelimiter).length
      Range(1, n+1).map(x=>"_"+x).toList
    case _ => (sourceMetaData_ \\ "field" \\ "@id").map(_.text)
  }

  private val mapMetaData_ = config_ \\ "mapMetaData"
  val targetDelimiter: String = mapMetaData_ \ "values" \ "@delimiter" text
  val targetFile: String = config_ \ "@outputPath" text
  //val targetFields = (mapMetaData_ \\ "field" \\ "@id").map(_.text)
  val targetFields: Seq[String] = {
    (mapMetaData_ \\ "field" \\ "@id").flatMap(item => {
      item.text match {
        case "*" => sourceFields
        case _ => item
      }
    })
  }.map(_.toString)

  private val seqfun_ : NodeSeq = config_ \\ "transform" \\ "function"
  private val seqfilter_ : NodeSeq = config_ \\ "transform" \\ "filter"

  def getMapper: Map[String, Mapper] = dimensionTable \\ "mapper" map (node => {
    (node \\ "@name" text, MapperFactory.create(node))
  }) toMap

  def getFunctions = {
    val functions = new ListBuffer[TransformDescription]
    val filters = new ListBuffer[FilterDescription]

    seqfun_.map(func =>{
      /*
      val funClass = func \ "@class" text match {
        case "" => "Functions"
        case s: String => s
      }

      val name = func \ "@name" text
*/
      val funClass = func \ "@name" text
      val className = ProcessConfig.funcPackageName + funClass
      val funobj = Class.forName(className).newInstance()
      val name = funobj match {
        //case o if o.isInstanceOf[Transformer] => "transform"
        case _ : Transformer => "transform"
        case _ : Filter => "filter"
        case _ =>
          logError("Illegal Class Name")
          throw new ConfigurationException("Function class")
      }

      val paramList = func.attributes.
        filter(attr => isParameter(attr.key)).
        map(n => (n.key, n.value.text)).toList

      val outputList = {
        val r = func.attributes.
          filter (attr => isOutputAttr (attr.key) ).
          value.text.split (",").map (_.trim).toList
        r.head match {
          case "*" => sourceFields.toList
          case _ => r
        }
      }

      name match {
        case "filter" => filters += new FilterDescription(name, className, paramList, outputList)
        case _ => functions += new TransformDescription(name, className, paramList, outputList)
      }
    })
    (functions.toList, filters.toList)
  }

  private def isNameAttr(s: String): Boolean = s == nameAttribute
  private def isOutputAttr(s: String): Boolean = s == outputAttribute
  private def isParameter(s: String): Boolean = (s != nameAttribute) && (s != outputAttribute)
  private def fetchOneLine(f: String): String  = Source.fromFile(f).getLines.next()
}

object ProcessConfig {
  val funcPackageName = "com.gsta.datahub.etl."
}

