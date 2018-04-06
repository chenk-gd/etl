package dh.etl

import org.apache.spark.Logging
import scala.collection.mutable
import scala.reflect.runtime.universe.MethodMirror

/**
  * Created by Administrator on 2015/5/26.
  */
abstract class FunctionDescription(val funcName: String,
                                   val className: String,
                                   val paramList: List[(String, String)],
                                   val outputList: List[String])
  extends Logging {
  val (paramTag, paramName) = paramList.unzip
  var currentFields: List[String] = _

  import scala.reflect.runtime.{universe => ru}

  private val c = Class.forName(className)
  private val funobj = Class.forName(className).newInstance()

  protected val methodMirror: MethodMirror = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val classType = m.classSymbol(c).toType
    // val sym = classType.declaration(ru.newTermName(funcName)).asMethod
    val sym = classType.decl(ru.TermName(funcName)).asMethod
    val im = m.reflect(funobj)
    im.reflectMethod(sym)
  }

  private val paramTypes = c.getMethods.
    filter(_.getName == funcName)(0).getParameterTypes.toList

  /*testing*/
  val paramNs: Class[_] = c.getMethods()(0).getReturnType
  logInfo("name = " + funcName + " className = " + className)

  def getParamTypes: List[Class[_]] = paramTypes

  def getParams(pc: ProcessContext, data: mutable.HashMap[String, String]): Array[Any] = {
    paramName.zip(paramTypes).map(pair => {
      val (p, t) = pair
      p.trim match {
        case x if x startsWith "%" =>
          pc.mapper.get(x.substring(1)) match {
            case None =>
              logError("Mapper: " + x + " not available")
              throw new ConfigurationException("Mapper")
            case Some(value) => value
          }
        case x if x startsWith "$" =>
          val v = data.get(x.substring(1)) match {
            case None =>
              logError("Parameter: " + x + " not available")
              throw new ConfigurationException("Input Parameter")
            case Some(value) => value
          }
          castTo(v, t)
        case x if x == "*" =>
          pc.sourceFields.map(f => {
            data.get(f) match {
              case None =>
                logError("Source fields configuration error")
                throw new ConfigurationException("Source fields")
              case Some(value) => value
            }
          }).toArray
        case x if x == "**" =>
          currentFields = data.keys.toList
          currentFields.map(f => data.getOrElse(f, "impossible")).toArray
        case x if x.startsWith("[") && x.endsWith("]") => {
          val tx = x.substring(1, x.length - 1)
          //Not yet implemented
        }
        case x => castTo(x, t)
      }
    }).toArray
  }

  protected def castTo(s: String, t: Class[_]) : Any = t match {
    // maybe wrong
    case `c` if c == classOf[Double] => s.trim.toDouble
    case `c` if c == classOf[Int] => s.trim.toInt
    case `c` if c == classOf[String] => s.trim
    case _ => s
  }

  override def toString: String = {
    val kvs = paramList.map {
      case (k, v) => k + "=" + v
    }.mkString(",")

    funcName + "(" + kvs + ") : (" + outputList.mkString(",") + ")"
  }

  def apply(pc: ProcessContext, data: mutable.HashMap[String, String]) : Boolean = {
    val params = getParams(pc, data)
    compute(params, data)
  }

  def compute(params: Array[_], data: mutable.HashMap[String, String]): Boolean
}
