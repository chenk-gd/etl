package dh.etl

import scala.collection.mutable

/**
  * Created by Administrator on 2015/5/27.
  */
class TransformDescription(name: String,
                           className: String,
                           paramList: List[(String, String)],
                           outputList: List[String])
  extends FunctionDescription(name, className, paramList, outputList) {

  def compute(params: Array[_], data: mutable.HashMap[String, String]) : Boolean = {
    logDebug(params.map(_.getClass).mkString(","))
    val r = methodMirror.apply(params: _*) match {
      case x: List[_] => x
      case x => List(x)
    }

    (outputList.head match {
      case "**" => currentFields
      case _ => outputList
    }).zip(r).foreach {
      case (k, v) => data(k) = v.toString
    }

    true
  }
}
