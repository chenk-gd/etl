package dh.etl

import scala.collection.mutable

/**
  * Created by Administrator on 2015/5/27.
  */
class FilterDescription(name: String,
                        className: String,
                        paramList: List[(String, String)],
                        outputList: List[String])
  extends FunctionDescription(name, className, paramList, outputList) {
  def compute(params: Array[_], data: mutable.HashMap[String, String]) : Boolean = {
    logDebug(params.map(_.getClass).mkString(","))
    methodMirror.apply(params: _*).asInstanceOf[Boolean]
  }
}
